import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import sys
import os
import threading
import queue
import time
import shutil

# Attempt to load project modules
MODULES_LOADED = False
try:
    from processor_mpcorb import config as MPCProcessorConfig, processor as MPCProcessor
    from processor_neo import config as NEOProcessorConfig, processor as NEOProcessor
    from merger import config as MergerConfig, merger as DataMerger
    from importer import importer as DBImporter
    MODULES_LOADED = True
except ImportError as e:
    IMPORT_ERROR = str(e)

# --- Output Redirection ---
class QueueWriter:
    """Redirects stdout/stderr to a thread-safe queue."""
    def __init__(self, queue):
        self.queue = queue

    def write(self, text):
        self.queue.put(text)

    def flush(self):
        pass

# --- Main Application ---
class AsteroidPipelineApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Asteroid Data Pipeline Controller")
        self.root.geometry("1100x800")
        self.root.minsize(900, 700)

        # Threading & Logging
        self.log_queue = queue.Queue()
        self.is_running = False

        # Theme Colors
        self.colors = {
            "bg": "#2b2b2b",
            "panel": "#3c3f41",
            "fg": "#e0e0e0",
            "accent": "#007acc",
            "success": "#388e3c", # Darker Green
            "warning": "#d32f2f", # Darker Red
            "text_bg": "#1e1e1e",
            "header": "#ffffff"
        }

        # Setup UI
        self._setup_styles()
        self.root.configure(bg=self.colors["bg"])
        self._create_layout()
        self._init_logging()

        # Initial checks
        if not MODULES_LOADED:
            self.log_message(f"[CRITICAL] Failed to import project modules: {IMPORT_ERROR}\n")
            self.log_message("Make sure you are running this script from the root of the project.\n")
            self._disable_controls()
        else:
            self.log_message("Ready. Project modules loaded successfully.\n")
            self.load_env_config()

        # Start UI updater loop
        self.root.after(100, self._process_log_queue)

    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')

        # General Styles
        style.configure(".", background=self.colors["bg"], foreground=self.colors["fg"], font=("Segoe UI", 10))
        style.configure("TFrame", background=self.colors["bg"])
        style.configure("TPanedwindow", background=self.colors["bg"])

        # Label Frames (Panels)
        style.configure("Card.TLabelframe", background=self.colors["panel"], relief="solid", borderwidth=1, bordercolor="#555")
        style.configure("Card.TLabelframe.Label", background=self.colors["panel"], foreground=self.colors["header"], font=("Segoe UI", 11, "bold"))

        # Frame inside panels needs to match panel color
        style.configure("Panel.TFrame", background=self.colors["panel"])

        # Buttons
        style.configure("TButton", padding=8, relief="flat", borderwidth=0, font=("Segoe UI", 9, "bold"))

        # Primary Button (Run All)
        style.configure("Primary.TButton", background=self.colors["success"], foreground="white", font=("Segoe UI", 12, "bold"))
        style.map("Primary.TButton", background=[('active', '#4caf50'), ('disabled', '#2e5c31')])

        # Step Buttons
        style.configure("Step.TButton", background=self.colors["accent"], foreground="white")
        style.map("Step.TButton", background=[('active', '#2196f3'), ('disabled', '#1a4e75')])

        # Danger Button
        style.configure("Danger.TButton", background=self.colors["warning"], foreground="white")
        style.map("Danger.TButton", background=[('active', '#f44336'), ('disabled', '#752828')])

        # Labels
        style.configure("MainHeader.TLabel", font=("Segoe UI", 18, "bold"), foreground="white", padding=10)
        style.configure("Status.TLabel", background=self.colors["accent"], foreground="white", font=("Segoe UI", 10), padding=5)

    def _create_layout(self):
        # Header
        header_frame = ttk.Frame(self.root)
        header_frame.pack(fill=tk.X)
        ttk.Label(header_frame, text="🚀 Asteroid Data Pipeline", style="MainHeader.TLabel").pack(side=tk.LEFT)

        # Main Split Container
        paned_window = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))

        # --- LEFT COLUMN: CONTROLS ---
        left_panel = ttk.Frame(paned_window)
        # Pack wrapper isn't needed for paned window usually, but helps with padding if we used .add directly on Labelframes

        # 1. Pipeline Actions Area
        actions_frame = ttk.LabelFrame(left_panel, text=" Pipeline Actions ", style="Card.TLabelframe", padding=15)
        actions_frame.pack(fill=tk.X, pady=(0, 15))

        # Panel content frame to set background correct
        act_content = ttk.Frame(actions_frame, style="Panel.TFrame")
        act_content.pack(fill=tk.BOTH, expand=True)

        self.btn_run_all = ttk.Button(act_content, text="▶ START FULL PIPELINE", style="Primary.TButton", command=self.start_full_pipeline)
        self.btn_run_all.pack(fill=tk.X, pady=(0, 15))

        # Grid for individual steps
        step_grid = ttk.Frame(act_content, style="Panel.TFrame")
        step_grid.pack(fill=tk.X)
        step_grid.columnconfigure(0, weight=1)
        step_grid.columnconfigure(1, weight=1)

        self.btn_step1 = ttk.Button(step_grid, text="1. Process MPCORB", style="Step.TButton", command=lambda: self.start_single_step("MPCORB"))
        self.btn_step1.grid(row=0, column=0, sticky="ew", padx=(0, 5), pady=5)

        self.btn_step2 = ttk.Button(step_grid, text="2. Process NEO", style="Step.TButton", command=lambda: self.start_single_step("NEO"))
        self.btn_step2.grid(row=0, column=1, sticky="ew", padx=(5, 0), pady=5)

        self.btn_step3 = ttk.Button(step_grid, text="3. Merge Data", style="Step.TButton", command=lambda: self.start_single_step("MERGE"))
        self.btn_step3.grid(row=1, column=0, sticky="ew", padx=(0, 5), pady=5)

        self.btn_step4 = ttk.Button(step_grid, text="4. Import DB", style="Step.TButton", command=lambda: self.start_single_step("IMPORT"))
        self.btn_step4.grid(row=1, column=1, sticky="ew", padx=(5, 0), pady=5)

        # Maintenance Area
        maint_frame = ttk.LabelFrame(left_panel, text=" Maintenance ", style="Card.TLabelframe", padding=15)
        maint_frame.pack(fill=tk.X, pady=(0, 15))
        maint_content = ttk.Frame(maint_frame, style="Panel.TFrame")
        maint_content.pack(fill=tk.BOTH)

        self.btn_clean = ttk.Button(maint_content, text="🗑 Clean Output Directories", style="Danger.TButton", command=self.clean_directories)
        self.btn_clean.pack(fill=tk.X)

        # Configuration Area
        config_frame = ttk.LabelFrame(left_panel, text=" Configuration (.env) ", style="Card.TLabelframe", padding=15)
        config_frame.pack(fill=tk.BOTH, expand=True)
        config_content = ttk.Frame(config_frame, style="Panel.TFrame")
        config_content.pack(fill=tk.BOTH, expand=True)

        self.txt_config = scrolledtext.ScrolledText(
            config_content,
            height=8,
            font=("Consolas", 10),
            bg=self.colors["text_bg"],
            fg="#dcdcdc",
            insertbackground="white",
            relief="flat",
            borderwidth=1
        )
        self.txt_config.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        btn_conf_row = ttk.Frame(config_content, style="Panel.TFrame")
        btn_conf_row.pack(fill=tk.X)
        ttk.Button(btn_conf_row, text="↻ Reload", command=self.load_env_config).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_conf_row, text="💾 Save Changes", command=self.save_env_config).pack(side=tk.LEFT)

        paned_window.add(left_panel, weight=1)

        # --- RIGHT COLUMN: LOGS ---
        right_panel = ttk.LabelFrame(paned_window, text=" System Console ", style="Card.TLabelframe", padding=10)

        # Log Text Area
        self.txt_log = scrolledtext.ScrolledText(
            right_panel,
            state='disabled',
            font=("Consolas", 10),
            bg=self.colors["text_bg"],
            fg="#a9b7c6",
            insertbackground="white",
            relief="flat",
            selectbackground="#214283"
        )
        self.txt_log.pack(fill=tk.BOTH, expand=True)

        paned_window.add(right_panel, weight=2)

        # --- STATUS BAR ---
        status_bar = ttk.Frame(self.root, style="Status.TLabel") # Using label style for background
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(status_bar, textvariable=self.status_var, background=self.colors["accent"], foreground="white").pack(side=tk.LEFT, padx=10)

        self.progress_bar = ttk.Progressbar(status_bar, mode='indeterminate', length=200)
        self.progress_bar.pack(side=tk.RIGHT, padx=10, pady=2)

    def _init_logging(self):
        self.queue_writer = QueueWriter(self.log_queue)

    def _process_log_queue(self):
        while not self.log_queue.empty():
            try:
                text = self.log_queue.get_nowait()
                self.txt_log.configure(state='normal')
                self.txt_log.insert(tk.END, text)
                self.txt_log.see(tk.END)
                self.txt_log.configure(state='disabled')
            except queue.Empty:
                break
        self.root.after(100, self._process_log_queue)

    def log_message(self, message):
        self.log_queue.put(message + "\n")

    def _disable_controls(self):
        state = 'disabled'
        self.btn_run_all.configure(state=state)
        self.btn_step1.configure(state=state)
        self.btn_step2.configure(state=state)
        self.btn_step3.configure(state=state)
        self.btn_step4.configure(state=state)
        self.btn_clean.configure(state=state)

    def _enable_controls(self):
        state = 'normal'
        self.btn_run_all.configure(state=state)
        self.btn_step1.configure(state=state)
        self.btn_step2.configure(state=state)
        self.btn_step3.configure(state=state)
        self.btn_step4.configure(state=state)
        self.btn_clean.configure(state=state)

    # --- Config ---
    def load_env_config(self):
        env_path = '.env'
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                content = f.read()
            self.txt_config.delete("1.0", tk.END)
            self.txt_config.insert("1.0", content)
            self.log_message(f"[INFO] Loaded .env configuration.")
        else:
            self.log_message(f"[WARN] .env file not found.")

    def save_env_config(self):
        env_path = '.env'
        content = self.txt_config.get("1.0", tk.END).strip()
        try:
            with open(env_path, 'w') as f:
                f.write(content)
            self.log_message(f"[SUCCESS] Saved .env configuration.")
            for line in content.splitlines():
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")
            self.log_message("[INFO] Environment variables updated in memory.")
        except Exception as e:
            self.log_message(f"[ERROR] Failed to save .env: {e}")

    # --- Execution Logic ---
    def start_full_pipeline(self):
        if self.is_running: return
        self._start_thread(self._run_full_pipeline_logic, "Running Full Pipeline")

    def start_single_step(self, step_name):
        if self.is_running: return
        self._start_thread(lambda: self._run_single_step_logic(step_name), f"Running Step: {step_name}")

    def clean_directories(self):
        if self.is_running: return
        self._start_thread(self._run_clean_logic, "Cleaning Directories")

    def _start_thread(self, target_func, description):
        self.is_running = True
        self._disable_controls()
        self.status_var.set(f"⏳ {description}...")
        self.progress_bar.start(10)

        thread = threading.Thread(target=self._thread_wrapper, args=(target_func,))
        thread.daemon = True
        thread.start()

    def _thread_wrapper(self, target_func):
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        sys.stdout = self.queue_writer
        sys.stderr = self.queue_writer

        try:
            target_func()
        except Exception as e:
            print(f"\n[CRITICAL ERROR] Execution failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            self.root.after(0, self._on_task_complete)

    def _on_task_complete(self):
        self.is_running = False
        self._enable_controls()
        self.status_var.set("✅ Ready")
        self.progress_bar.stop()
        self.log_message("\n=== Process Completed ===\n")

    # --- Pipeline Logic ---
    def _run_clean_logic(self):
        print("--- Cleaning Directories ---")
        dirs_to_clean = [
            MPCProcessorConfig.OUTPUT_DIR,
            NEOProcessorConfig.OUTPUT_DIR,
            MergerConfig.OUTPUT_DIR
        ]
        for d in dirs_to_clean:
            if os.path.exists(d):
                try:
                    shutil.rmtree(d)
                    print(f"  [CLEAN] Removed: {d}")
                except OSError as e:
                    print(f"  [WARN] Failed to remove {d}: {e}")
            else:
                print(f"  [INFO] Not found (already clean): {d}")

    def _run_single_step_logic(self, step):
        if step == "MPCORB":
            if os.path.exists(MPCProcessorConfig.INPUT_FILE):
                print(f"\n[{step}] Starting Processing...")
                MPCProcessor.AsteroidProcessor(MPCProcessorConfig.INPUT_FILE, MPCProcessorConfig.OUTPUT_DIR).process()
            else:
                print(f"[ERROR] Input file not found: {MPCProcessorConfig.INPUT_FILE}")

        elif step == "NEO":
            if os.path.exists(NEOProcessorConfig.INPUT_FILE):
                print(f"\n[{step}] Starting Processing...")
                NEOProcessor.AsteroidProcessor(NEOProcessorConfig.INPUT_FILE, NEOProcessorConfig.OUTPUT_DIR).process()
            else:
                print(f"[ERROR] Input file not found: {NEOProcessorConfig.INPUT_FILE}")

        elif step == "MERGE":
            print(f"\n[{step}] Starting Merge...")
            DataMerger.DataMerger().run()

        elif step == "IMPORT":
            print(f"\n[{step}] Starting Database Import...")
            from importer import config as ImporterConfig
            if 'SQL_CONNECTION_STRING' in os.environ:
                 ImporterConfig.DB_CONNECTION_STRING = os.environ.get('SQL_CONNECTION_STRING')
            DBImporter.DBImporter().run()

    def _run_full_pipeline_logic(self):
        print("="*60)
        print("ASTEROID DATA PIPELINE AUTOMATION")
        print("="*60)
        self._run_clean_logic()
        self._run_single_step_logic("MPCORB")
        self._run_single_step_logic("NEO")
        self._run_single_step_logic("MERGE")
        self._run_single_step_logic("IMPORT")
        print("\n" + "="*60)
        print(f"FULL PIPELINE COMPLETE.")
        print("="*60)

if __name__ == "__main__":
    root = tk.Tk()
    app = AsteroidPipelineApp(root)
    root.mainloop()