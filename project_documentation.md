# DocParser API: Systems Architecture & Technical Specification
**A FastAPI-powered Multimodal Real Estate Document Extraction Pipeline**

---

## PAGE 1: DOCUMENT OVERVIEW & PROJECT CONTEXT

### 1.1 Abstract
The **DocParser API** is a production-grade, asynchronous document processing service designed to extract high-fidelity structured data, text (OCR), and segmented visual assets (floorplans, amenities, logos) from brochure-style PDF documents. Operating primarily in the real-estate domain, the service translates unstructured, visually-dense brochures into validated, domain-specific JSON payloads and organized asset folders. The backend utilizes FastAPI for asynchronous request management, PyMuPDF (fitz) for document processing, and the Google GenAI SDK to interact with Gemini models for vision and extraction tasks.

### 1.2 Core System Features
*   **Asynchronous Processing Pipeline**: Decouples API Ingestion from Execution. Requests receive an immediate tracking token while processing happens in the background.
*   **Multimodal Image Segmenter**: Renders PDF pages to images and classifies visual sub-regions using Gemini vision models, cropping and categorizing elements on a normalized Coordinate Space.
*   **High-Fidelity OCR Engine**: Transcribes textual content page-by-page, generating structured Markdown, with an automatic fallback mechanism to avoid copyrighted or restricted citation errors.
*   **Structured Schema Extractor**: Employs Pydantic schemas converted to JSON schemas to enforce structural validation on LLM extractions (such as properties, tower config, address, and amenities).
*   **Active Cost Tracking**: Calculates token usage dynamically across multiple processing stages and converts API costs to domestic currency (INR) using live forex feeds.
*   **Incremental Resume and Disk Hydration**: Minimizes redundant computation. Recoverable tasks automatically reconstruct their state upon application startup by scanning output directories.

### 1.3 Technical Stack
*   **API Framework**: FastAPI & Uvicorn (ASGI web server).
*   **PDF Core**: PyMuPDF (`fitz`) for rasterization, page counts, and structural parsing.
*   **AI Models**: `gemini-3-flash-preview` (default for visual detection and structured extraction), `gemini-flash-latest` (fallback for citation avoidance during OCR).
*   **Data Validation**: Pydantic v2.
*   **Helper Tools**: `yfinance` (real-time exchange rate parsing), `Pillow` (image resizing, cropping, and formatting).

### 1.4 Codebase Directory Structure
Below is the directory structure layout for the DocParser API project:

```
DocParser_api/
├── app.py                     # ASGI main entry point and routes definition
├── requirements.txt           # Package dependencies
├── schemas_all.py             # Predefined static validation schemas
├── extraction_test.py         # Testing wrapper script
├── modules/                   # Core business logic processing components
│   ├── image_module.py        # PDF image extraction and cropping engine
│   └── ocr_module.py          # PDF text OCR and page layout reconstruction
├── pipelines/                 # Composite pipeline orchestration
│   ├── extraction_pipeline.py # Structured JSON schema extraction
│   ├── image_pipeline.py      # PDF image processing pipeline
│   └── ocr_pipeline.py        # PDF OCR processing pipeline
├── utils/                     # Utility functions and configuration models
│   ├── build_prompt.py        # Dynamic prompt composition for image segmenter
│   ├── cost_tracker.py        # Multi-model token cost computation
│   ├── fx_rates.py            # Live exchange rate caching provider
│   ├── prompts.py             # System prompts for OCR and fallback processing
│   ├── pydantic_models.py     # Pydantic schemas, enums, and validators
│   └── parent_to_child.json   # Sub-category taxonomy mapping
├── workers/                   # Async task queue runners
│   └── parse_worker.py        # Background task worker and zip packager
└── frontend/                  # Web dashboard served at /app
    ├── index.html             # User interface HTML document
    ├── styles.css             # UI styling sheets
    └── app.js                 # Frontend API coordinator script
```

---

## PAGE 2: SYSTEM ARCHITECTURE & DATA FLOW

### 2.1 Component Interaction
The system is divided into four functional layers: the **Ingestion Layer** (FastAPI controllers), the **Orchestration Layer** (Background Task workers), the **Transformation Layer** (pipelines and processing modules), and the **Storage Layer** (disk-based file system).

```mermaid
graph TD
    Client[Client / Web UI] -- 1. POST /parse --> Ingestion[FastAPI Engine: app.py]
    Ingestion -- 2. Save PDF & Meta --> StorageJobs[storage/jobs/]
    Ingestion -- 3. Dispatch Async Job --> Worker[parse_worker.py]
    Ingestion -- 4. Return task_id --> Client

    subgraph Orchestration Layer
        Worker -- 5. Execute OCR --> OCR[ocr_pipeline.py]
        Worker -- 6. Execute Images --> IMG[image_pipeline.py]
        OCR -- 7. Parse Text --> OCRMod[ocr_module.py]
        IMG -- 8. Segment & Crop --> IMGMod[image_module.py]
        OCRMod -. 9. Write MD .- OutputOCR[storage/returns/task_id/ocr/]
        IMGMod -. 10. Write Crops .- OutputIMG[storage/returns/task_id/images/]
        
        OutputOCR --> ExtPipe[extraction_pipeline.py]
        Worker -- 11. Run Extraction --> ExtPipe
        ExtPipe -. 12. Write JSON .- OutputExt[storage/returns/task_id/extraction/]
    end

    OCRMod & IMGMod & ExtPipe -- Track Cost --> Tracker[cost_tracker.py]
    Tracker -- Fetch FX --> FX[fx_rates.py]
    Worker -- 13. Dump Cost Summary --> CostFile[storage/returns/task_id/cost_summary.json]
    Worker -- 14. Zip Outputs --> ZipFile[storage/returns/task_id.zip]
```

### 2.2 Sequence of Execution (Parse Job Lifecycle)
1.  **Request Ingestion**: The client submits a PDF file to `/parse`. The file is saved to [storage/jobs/](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py#L22) and an entry is initialized in the metadata database under [storage/task_meta/](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py#L24).
2.  **Concurrency Control**: The job runner checks the [job_semaphore](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py#L14). If the active job count exceeds `MAX_CONCURRENT_JOBS` (default: 30), the task waits in the ASGI queue.
3.  **OCR Execution (Stage 1)**: The [run_ocr](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/pipelines/ocr_pipeline.py#L5) pipeline runs asynchronously. The PDF is converted to page images and processed through Gemini to obtain a unified Markdown layout file.
4.  **Parallel Segmenter & Cropper (Stage 2)**: The [run_image_extraction](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/pipelines/image_pipeline.py#L5) is submitted to a thread pool (`IMAGE_EXECUTOR`). It classifies, detects boxes, and crops images in parallel with OCR execution.
5.  **Structured Information Extraction (Stage 3)**: Once Stage 1 finishes, if structured extraction is enabled, the [run_extraction](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/pipelines/extraction_pipeline.py#L22) pipeline consumes the OCR Markdown and queries Gemini with the structured schema.
6.  **Archiving and Summary Generation**: The worker gathers the outputs, writes a localized cost calculation file, packages the results into a single `.zip` file, and updates the task status to `done`.

---

## PAGE 3: API ROUTE REFERENCE & ENDPOINT SCHEMAS

The [app.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py) module hosts the FastAPI application definition and registers the route handles.

### 3.1 POST /parse
Submits a document for background parsing.
*   **Request Type**: `multipart/form-data`
*   **Parameters**:
    *   `file` (UploadFile, Required): The PDF file.
    *   `image_extraction` (Query, Boolean, Default: `True`): Extracts and crops page images if set.
    *   `brochure_extraction` (Query, Boolean, Default: `False`): Enables structural schema extraction using the standard brochure schema layout.
    *   `extraction_schema` (Form, String, Optional): A valid raw JSON Schema string describing custom structured data fields.
*   **Response (JSON)**:
    ```json
    {
      "task_id": "8c454e38-16e0-40e9-bbf2-11e2f778d2b2",
      "status": "processing",
      "image_extraction": true,
      "structured_extraction": true,
      "extraction_mode": "brochure"
    }
    ```

### 3.2 GET /status/{task_id}
Fetches the current lifecycle state of a specific extraction job.
*   **Response (JSON)**:
    ```json
    {
      "task_id": "8c454e38-16e0-40e9-bbf2-11e2f778d2b2",
      "status": "done",
      "image_extraction": true,
      "structured_extraction": true,
      "extraction_mode": "brochure",
      "zip_path": "storage/returns/8c454e38-16e0-40e9-bbf2-11e2f778d2b2.zip"
    }
    ```

### 3.3 GET /download/{task_id}
Downloads the compiled `.zip` file once the task status is `done`.
*   **Response**: `200 OK` (binary attachment stream containing `<task_id>.zip`) or `400 Bad Request` if the job has not finished, or `404 Not Found` if the archive is missing.

### 3.4 GET /cost/{task_id}
Returns the total API cost calculation summary for the task.
*   **Response (JSON)**:
    ```json
    {
      "task_id": "8c454e38-16e0-40e9-bbf2-11e2f778d2b2",
      "currency": "INR",
      "total_cost_inr": 8.42
    }
    ```

### 3.5 System Boot and Task Rehydration
To survive server restarts, the application invokes [rehydrate_tasks()](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py#L52) during startup:
*   It scans the directory [storage/returns/](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/app.py#L23) for existing `.zip` files.
*   For every zip file, it extracts the `task_id` and adds it back to the in-memory `TASKS` cache with status `done`, generating a recovery metadata file on disk to guarantee route accessibility.

---

## PAGE 4: OCR & TEXT EXTRACTION PIPELINE

The OCR and text extraction logic is split between the pipeline layer [pipelines/ocr_pipeline.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/pipelines/ocr_pipeline.py) and the core module [modules/ocr_module.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/ocr_module.py).

### 4.1 Preprocessing and Page Rasterization
*   Because PDFs might contain complex mixed vector elements and text layouts, the pipeline rasterizes pages using PyMuPDF [split_pdf_to_pages()](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/ocr_module.py#L56).
*   Pages are rendered to PNG files using a zoom matrix configured to **150 DPI** (Zoom Factor = 150/72 = 2.0833) to balance character clarity and payload size:
    ```python
    zoom = 150 / 72
    matrix = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=matrix, alpha=False)
    ```
*   Images are saved to a temporary path (`/tmp/pdf_pages_...`) and mapped to their respective page numbers.

### 4.2 Gemini File API Lifecycle
To maintain API stability, pages are processed concurrently but upload operations are regulated:
1.  **Retry-based Ingestion**: Page images are copied to a temporary file structure and uploaded using [upload_with_retry_blocking()](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/ocr_module.py#L91).
2.  **State Verification**: The system queries the uploaded file status, waiting up to 60 seconds for the state to transition to `ACTIVE` before initiating processing.
3.  **Strict Cleanup**: After OCR execution completes or fails, [delete_uploaded_file_blocking()](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/ocr_module.py#L199) is called to delete the temporary image asset from Google's servers to avoid storage accumulation.

### 4.3 OCR Prompting & Fallback Strategies
To support high quality OCR while bypassing copyright blocking rules, a two-tier model approach is used:
*   **Standard Prompt ([ocr_prompt](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/prompts.py#L139))**: Targets verbatim transcription. Instructs the model to output markdown layouts, tables, headings, and a factual summary.
*   **Fallback Prompt ([fallback_ocr_prompt](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/prompts.py#L376))**: Automatically invoked if `gemini-3-flash-preview` raises citation/recitation errors. The model falls back to `gemini-flash-latest` and runs with instructions to paraphrase text while retaining all numeric values.
*   **Thinking Config Selection**:
    *   For `gemini-flash-latest`, a `thinking_budget` of 300 tokens is allocated.
    *   For `gemini-3-flash-preview`, `thinking_level` is set to `"low"`.

### 4.4 Incremental Resume Mechanism
When [ocr_single_pdf_async()](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/ocr_module.py#L252) executes, it checks for an existing `<pdf_name>_ocr.json` file in the output directory.
*   If the page count of the PDF matches the length of the JSON array, processing is skipped.
*   If a mismatch occurs, the pipeline loads already completed page entries, and only spawns API worker tasks for missing page numbers, preventing redundant API costs.

---

## PAGE 5: MULTIMODAL IMAGE PARSING & CROP ENGINE

The image segmenter in [modules/image_module.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/modules/image_module.py) locates, classifies, and crops sub-images within PDF pages.

### 5.1 Dynamic Prompts & Taxonomy Definition
The prompt builder in [utils/build_prompt.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/build_prompt.py) generates system instructions including classification guidelines, logo limits, and cropping rules.

```
Taxonomy Classes (PARENT_CLASSES):
├── floorplan          # Typical Unit layouts, dimensional plans
├── floorplate         # Complete tower plate plans
├── masterplan         # Project layout and site plan blueprints
├── amenities          # Facility photos (e.g. pools, gyms)
├── location_plan      # Nearby geography and road maps
├── outdoors           # Building rendering, landscape, facade photos
├── demo_flat          # Interior display suite photography
├── contact_information# Graphic contact blocks with phone numbers
├── project_logo       # Marketing logos representing the project
├── builder_logo       # Corporate brand logo of the developer
└── qr_code            # RERA certificate QR codes
```

### 5.2 Multimodal Object Detection Schema
Detection coordinates are requested via a Pydantic schema compiled into a Gemini validation contract:
```python
"box_2d": types.Schema(
    type=types.Type.ARRAY,
    items=types.Schema(type=types.Type.INTEGER),
    description="[ymin, xmin, ymax, xmax], scaled 0–1000"
)
```
*   Gemini maps normalized bounding coordinates onto a integer space from **0 to 1000** relative to the page.

### 5.3 PIL Crop Execution & Memory Protections
*   Once coordinates are returned, the script converts them to absolute image pixels:
    ```python
    left = int((xmin / 1000) * W)
    top = int((ymin / 1000) * H)
    right = int((xmax / 1000) * W)
    bottom = int((ymax / 1000) * H)
    ```
*   **Memory Limit Protection**: To prevent Out Of Memory (OOM) failures with large images, if a page's pixel area exceed **180,000,000 pixels**, PIL scales the image down using bilinear interpolation:
    ```python
    scale = (180_000_000 / (W * H)) ** 0.5
    image = image.resize((int(W * scale), int(H * scale)), Image.Resampling.BILINEAR)
    ```

### 5.4 Logo Extraction Filtering Rule
To avoid capturing repetitive header/footer branding elements across document pages:
*   `project_logo` and `builder_logo` elements are **ignored** if detected on pages other than **Page 1** (cover) or the **Last Page** of the brochure:
    ```python
    if cls in {"project_logo", "builder_logo"}:
        if page_number not in (1, total_pages):
            continue
    ```

---

## PAGE 6: STRUCTURED SCHEMAS & PYDANTIC VALIDATIONS

The structured extraction pipeline in [pipelines/extraction_pipeline.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/pipelines/extraction_pipeline.py) uses Pydantic validation schemas defined in [utils/pydantic_models.py](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/pydantic_models.py).

### 6.1 Brochure Validation Schema Structure
The core [brochureSchema](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/pydantic_models.py#L857) represents the target contract for Gemini JSON extraction:

| Module Field | Target Type | Validation / Extraction Description |
| :--- | :--- | :--- |
| `projectDetails` | Class Object | Houses `projectName`, `reraNo`, address details, design theme, coordinates. |
| `builderDetails` | Class Object | Details the `builderName`, website URLs, and logo typography styles. |
| `floorplanConfigs`| Array List | Captures unit area configurations (bhkType, superBuiltupArea, carpetArea). |
| `masterplanElements`| Class Object | Summarizes project size: total area, open area, green metrics, and tower counts. |
| `towerData` | Array List | Compiles tower-specific details (tower name, floor count, lift count). |
| `amenities` | Enum List | Array of strings validated against [amen_list](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/pydantic_models.py#L181) (227 valid values). |
| `locationHighlights`| Array List | Validates nearby points of interest and distances (Km/mins format). |
| `interiorSpecifications`| Class Object | Details architectural fittings, finishes, structural qualities. |
| `uspDetails` | Array List | Features and unique selling points, capped between 5 and 7 items. |
| `bankDetails` | Class Object | Identifies approved loan financing banks and escrow collection banks. |

### 6.2 Custom Validation Rules (Field Validators)
The model enforces formatting constraints through Pydantic field validators:
*   **Geographic Coordinates**: Restricts Latitude and Longitude to standard ranges (-90 to 90 for latitude, -180 to 180 for longitude) using strict regex matching:
    ```python
    latitude: pattern="^-?([1-8]?\d(\.\d+)?|90(\.0+)?)$"
    longitude: pattern="^-?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$"
    ```
*   **Location Highlights Format**: The system checks metrics to ensure they are represented in either Kilometers or minutes:
    ```python
    @field_validator("subHeading")
    def validate_metric(cls, value: str):
        if not re.match(r"^\d+(\.\d+)?\sKm$", value) and not re.match(r"^\d+\smins$", value):
            raise ValueError("subHeading must be in format '<number> Km' or '<number> mins'")
        return value
    ```
*   **USP Tag Validation**: Compares subcategories against predefined sets ([AMENITIES_SUBCATEGORIES](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/pydantic_models.py#L135) or [LOCATION_SUBCATEGORIES](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/pydantic_models.py#L171)) depending on the parent category value.

---

## PAGE 7: OPERATIONS, COST TRACKING & DISK STORAGE

Operational management involves runtime cost tracking and organizing temporary files on disk.

### 7.1 Dynamic Token-Based Cost Tracker
The [CostTracker](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/cost_tracker.py#L24) class monitors token usage across pipelines.
*   **Model Pricing Database**:
    *   `gemini-3-flash-preview`: Input USD $0.0005/1K tokens, Output USD $0.003/1K tokens.
    *   `gemini-2.5-flash`: Input USD $0.0003/1K tokens, Output USD $0.0025/1K tokens.
    *   `gemini-3.1-pro-preview` (Tiered Pricing): Input USD $0.002/1K tokens (≤200K tokens) scaling to USD $0.004/1K tokens (>200K tokens).
*   **Exchange Rate Provider**: The [FXRateProvider](file:///C:/Users/suryansh.naithani/OneDrive%20-%20Info%20Edge%20%28India%29%20Ltd/Desktop/June%202026/DocParser_api/utils/fx_rates.py#L4) fetches USD/INR rates using `yfinance` (`USDINR=X`). Rates are cached for **60 seconds** to avoid hitting query limits:
    ```python
    ticker = yf.Ticker("USDINR=X")
    data = ticker.history(period="1d")
    self._rate = float(data['Close'].iloc[-1])
    ```

### 7.2 Directory Organization & Storage Layout
All job files are stored in the `storage/` directory inside the project workspace:
```
storage/
├── jobs/
│   └── {task_id}.pdf                      # Uploaded PDF file
├── task_meta/
│   └── {task_id}.json                     # Task status and parameters metadata
└── returns/
    ├── {task_id}.zip                      # Completed archive file for download
    └── {task_id}/                         # Decompressed workspace folder
        ├── cost_summary.json              # Final USD/INR cost breakdown
        ├── extraction/
        │   └── extracted_data.json        # Structured JSON extraction output
        ├── ocr/
        │   └── {pdf_name}/
        │       ├── {pdf_name}_ocr.json    # Page-by-page JSON array text
        │       └── {pdf_name}_ocr.md      # Unified Markdown document
        └── images/
            ├── final_output.json          # Crop bounds and metadata mapping
            ├── floorplan/                 # Extracted floorplan crop image files
            ├── outdoors/                  # Extracted rendering crop image files
            └── amenities/                 # Extracted amenity crop image files
```

### 7.3 Output ZIP Architecture
The packaged ZIP archive reorganizes outputs into a simplified layout:
*   `ocr.md` (unified text Markdown document)
*   `ocr.json` (raw JSON transcription array)
*   `parse.json` (image classification mapping output)
*   `extracted_data.json` (Pydantic-validated brochure data JSON file, if structured extraction was run)
*   `images/{class_name}/{label}_{hash}.jpg` (cropped visual assets sorted by type)
