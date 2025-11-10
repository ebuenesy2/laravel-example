<?php
// Laravel 10+ compatible
// Dosyalar: migrations, modeller, servis, artisan command

/*
Migration: create_invalid_products_table
*/

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateInvalidProductsTable extends Migration
{
    public function up()
    {
        Schema::create('invalid_products', function (Blueprint $table) {
            $table->id();
            $table->string('external_id')->nullable();
            $table->json('payload')->nullable();
            $table->json('errors');
            $table->timestamps();
        });
    }

    public function down()
    {
        Schema::dropIfExists('invalid_products');
    }
}

/*
Migration: create_import_checkpoints_table
*/

class CreateImportCheckpointsTable extends Migration
{
    public function up()
    {
        Schema::create('import_checkpoints', function (Blueprint $table) {
            $table->id();
            $table->string('name')->unique(); // örn: thirdparty_products_default
            $table->unsignedBigInteger('last_page')->default(0);
            $table->json('meta')->nullable();
            $table->timestamp('last_processed_at')->nullable();
            $table->timestamps();
        });
    }

    public function down()
    {
        Schema::dropIfExists('import_checkpoints');
    }
}

/*
Model: InvalidProduct
*/

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class InvalidProduct extends Model
{
    protected $table = 'invalid_products';
    protected $guarded = [];
    protected $casts = [
        'payload' => 'array',
        'errors' => 'array',
    ];
}

/*
Model: ImportCheckpoint
*/

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class ImportCheckpoint extends Model
{
    protected $table = 'import_checkpoints';
    protected $guarded = [];
    protected $casts = [
        'meta' => 'array',
        'last_processed_at' => 'datetime',
    ];
}

/*
Service: ThirdPartyProductImporter
- Kullanımı: app/Services/ThirdPartyProductImporter.php
- Açıklama:
    * API sayfalandırılmış veri döner (100 öğe/page)
    * Rate limit: 10 req/min -> 1 istek / 6 saniye
    * Geçersiz ürünleri InvalidProduct tablosuna kaydeder
    * Başarısızlıklarda checkpoint kullanarak kaldığı yerden devam eder
*/

namespace App\Services;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;
use App\Models\InvalidProduct;
use App\Models\ImportCheckpoint;
use Exception;

class ThirdPartyProductImporter
{
    protected string $apiBase;
    protected int $pageSize = 100;
    protected int $rateLimitPerMinute = 10; // sabit
    protected float $minDelaySeconds;

    public function __construct(string $apiBase)
    {
        $this->apiBase = rtrim($apiBase, '/');
        $this->minDelaySeconds = 60 / $this->rateLimitPerMinute; // 6 saniye
    }

    /**
     * Başlat
     * @param string $sourceName checkpoint kayıt adı
     * @param bool $resume kaldığı yerden devam et
     */
    public function run(string $sourceName = 'default', bool $resume = true)
    {
        Log::info("Importer started for source={$sourceName}");

        $checkpoint = ImportCheckpoint::firstOrCreate(
            ['name' => "thirdparty_products_{$sourceName}"],
            ['last_page' => 0]
        );

        $startPage = $resume ? ($checkpoint->last_page + 1) : 1;

        $page = max(1, (int) $startPage);
        $requests = 0;

        try {
            while (true) {
                $startTime = microtime(true);

                Log::info("Fetching page={$page}");

                $response = $this->safeRequest("/products", ['page' => $page, 'limit' => $this->pageSize]);

                if (!$response || $response->failed()) {
                    // Eğer 5xx gibi geçici hata varsa retry'ler zaten safeRequest'te deniyor.
                    Log::error("İstek başarısız sayfa={$page}", ['status' => $response?->status()]);
                    // checkpoint'i güncelle ve çık
                    $checkpoint->last_page = $page - 1;
                    $checkpoint->last_processed_at = now();
                    $checkpoint->save();
                    break;
                }

                $data = $response->json();

                // API yapısına göre değişebilir: burada items ve totalPages varsayıyoruz
                $items = $data['items'] ?? $data['data'] ?? [];
                $totalPages = $data['total_pages'] ?? $data['totalPages'] ?? null;

                if (empty($items)) {
                    Log::info("Sayfa boş veya items yok. page={$page}. Process finished.");
                    $checkpoint->last_page = $page - 1;
                    $checkpoint->last_processed_at = now();
                    $checkpoint->save();
                    break;
                }

                foreach ($items as $item) {
                    $this->processItem($item);
                }

                // başarıyla işlediğimiz sayfayı checkpoint'e yaz
                $checkpoint->last_page = $page;
                $checkpoint->last_processed_at = now();
                $checkpoint->save();

                // eğer totalPages varsa ve son sayfadaysak kır
                if ($totalPages !== null && $page >= (int) $totalPages) {
                    Log::info("Tüm sayfalar işlendi. totalPages={$totalPages}");
                    break;
                }

                $page++;

                // rate limit için bekle
                $elapsed = microtime(true) - $startTime;
                if ($elapsed < $this->minDelaySeconds) {
                    $sleep = $this->minDelaySeconds - $elapsed;
                    Log::debug("Sleeping {$sleep} seconds to respect rate limit");
                    usleep((int)($sleep * 1_000_000));
                }
            }

            Log::info('Importer finished.');
        } catch (Exception $e) {
            Log::error('Importer crashed', ['exception' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            // checkpoint zaten güncellendi en son başarılı sayfa ile
            throw $e;
        }
    }

    protected function safeRequest(string $path, array $query = [], int $maxRetries = 3)
    {
        $attempt = 0;
        $backoff = 1; // saniye

        while ($attempt <= $maxRetries) {
            try {
                $attempt++;
                $response = Http::timeout(30)->get($this->apiBase . $path, $query);

                // 429 özel: eğer rate limit aşıldıysa header'dan bekleme süresi alınabilir
                if ($response->status() === 429) {
                    $retryAfter = (int) $response->header('Retry-After', 6);
                    Log::warning('API rate limited, waiting', ['retry_after' => $retryAfter]);
                    sleep(max(1, $retryAfter));
                    continue;
                }

                // 5xx durumlarında retry
                if ($response->serverError()) {
                    throw new Exception('Server error: ' . $response->status());
                }

                return $response;
            } catch (Exception $e) {
                Log::warning('Request attempt failed', ['attempt' => $attempt, 'error' => $e->getMessage()]);

                if ($attempt > $maxRetries) {
                    Log::error('Max retries reached for request', ['path' => $path, 'query' => $query]);
                    return null;
                }

                sleep($backoff);
                $backoff *= 2;
            }
        }

        return null;
    }

    protected function processItem(array $item): void
    {
        // Örnek doğrulama kuralları. Gerçek kuralları API'ye göre güncelle.
        $rules = [
            'id' => 'required|string',
            'sku' => 'required|string',
            'title' => 'required|string',
            'price' => 'required|numeric|min:0',
            'stock' => 'nullable|integer|min:0',
        ];

        $v = Validator::make($item, $rules);

        if ($v->fails()) {
            // geçersiz ürünü kaydet
            InvalidProduct::create([
                'external_id' => $item['id'] ?? null,
                'payload' => $item,
                'errors' => $v->errors()->toArray(),
            ]);

            Log::info('Invalid product saved', ['external_id' => $item['id'] ?? null, 'errors' => $v->errors()->toArray()]);
            return;
        }

        // buraya geçerli ürünü kaydetme/ güncelleme işlemi gelecek
        try {
            // ÖRNEK: App\Models\Product::updateOrCreate([...])
            // Product::updateOrCreate([
            //     'external_id' => $item['id'],
            // ], [
            //     'sku' => $item['sku'],
            //     'title' => $item['title'],
            //     'price' => $item['price'],
            //     'stock' => $item['stock'] ?? 0,
            //     'raw' => $item,
            // ]);

            // demo log
            Log::info('Valid product processed', ['external_id' => $item['id']]);
        } catch (Exception $e) {
            // ürün kaydedilirken hata olursa invalid olarak kaydet
            InvalidProduct::create([
                'external_id' => $item['id'] ?? null,
                'payload' => $item,
                'errors' => ['save' => [$e->getMessage()]],
            ]);
            Log::error('Failed to save product', ['external_id' => $item['id'] ?? null, 'error' => $e->getMessage()]);
        }
    }
}

/*
Artisan Command: ImportThirdPartyProducts
Komut: php artisan import:thirdparty-products {source?} {--no-resume}
*/

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Services\ThirdPartyProductImporter;
use Illuminate\Support\Facades\Log;

class ImportThirdPartyProducts extends Command
{
    protected $signature = 'import:thirdparty-products {source=default} {--no-resume : Do not resume from checkpoint}';
    protected $description = 'Üçüncü taraf API\'den ürünleri içe aktarır (sayfalandırılmış, rate limit ile uyumlu)';

    public function handle()
    {
        $source = $this->argument('source');
        $resume = !$this->option('no-resume');

        // API base URL'ini .env üzerinden almak iyi olur
        $apiBase = config('services.thirdparty.' . $source . '.base_url') ?? env('THIRD_PARTY_API_BASE');

        if (!$apiBase) {
            $this->error('API base URL bulunamadı. Lütfen THIRD_PARTY_API_BASE veya config/services.php içindeki ayarı kontrol edin.');
            return 1;
        }

        $this->info("Import started for source={$source}, resume=" . ($resume ? 'yes' : 'no'));

        try {
            $importer = new ThirdPartyProductImporter($apiBase);
            $importer->run($source, $resume);
            $this->info('Import completed.');
            return 0;
        } catch (\Exception $e) {
            Log::error('Import command failed', ['exception' => $e->getMessage()]);
            $this->error('Import sırasında hata: ' . $e->getMessage());
            return 1;
        }
    }
}

/*
NOTLAR ve Öneriler:
- migrations dosyalarını uygun klasörlere taşıyın (database/migrations).
- modelleri App/Models içine ekleyin.
- servisi App/Services içine ekleyin ve komutu App/Console/Commands içine koyun.
- .env içinde THIRD_PARTY_API_BASE veya config/services.php içine source bazlı konfigürasyon ekleyin.
- Product kaydı için kendi Product modelinizi kullanın (örnek kodda commented).
- Rate limit: basit ve güvenli bir bekleme mekanizması kullanıldı (1 istek/6s). API 'Retry-After' header'ı dönerse buna da uyacak şekilde safeRequest içinde ele alındı.
- Hataları ayrıntılı loglamak için storage/logs/laravel.log yeterli. Gerekirse Sentry veya başka bir hata izleme servisine bağlayın.
*/

?>
