import multiprocessing
import tkinter as tk
from tkinter import ttk, scrolledtext
import os
import sys
import queue

# Ensure we can import modules from the script's directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
	sys.path.insert(0, BASE_DIR)

# Project Root (Parent of Importer)
PROJECT_ROOT = os.path.dirname(BASE_DIR)

# --- WORKER PROCESS (Top-Level) ---
def run_worker_process(task_name, log_queue, env_vars, use_bulk=False):
	"""
	Executes the pipeline logic in a separate system process.
	This avoids GIL (Global Interpreter Lock) issues, preventing GUI freezes.
	"""
	# 1. Restore Environment
	import os
	import sys
	import shutil
	import time
	os.environ.update(env_vars)

	# 2. Redirect Print Statements to Queue
	class QueueWriter:
		def __init__(self, q): self.q = q
		def write(self, text): self.q.put(text)
		def flush(self): pass

	sys.stdout = QueueWriter(log_queue)
	sys.stderr = QueueWriter(log_queue)

	def set_status(msg):
		"""Helper to force a status bar update via the GUI's heuristic."""
		# The GUI looks for lines ending in \r to update the status bar
		print(f"{msg}\r")

	try:
		# 3. Import Modules (Lazy import ensures fresh state in new process)
		# We assume these are in the python path
		from processor_mpcorb import config as MPCProcessorConfig, processor as MPCProcessor
		from processor_neo import config as NEOProcessorConfig, processor as NEOProcessor
		from merger import config as MergerConfig, merger as DataMerger
		from importer import importer as DBImporter

		# 4. Helper for Cleaning
		def clean_dirs():
			print("--- Cleaning Directories ---")
			dirs = [MPCProcessorConfig.OUTPUT_DIR, NEOProcessorConfig.OUTPUT_DIR, MergerConfig.OUTPUT_DIR]
			for d in dirs:
				if os.path.exists(d):
					try:
						shutil.rmtree(d)
						print(f"  [CLEAN] Removed: {d}")
					except Exception as e:
						print(f"  [WARN] Failed: {d} - {e}")
				else:
					print(f"  [INFO] Already clean: {d}")

		# 5. Task Dispatch
		if task_name == "CLEAN":
			set_status("Cleaning directories...")
			clean_dirs()

		elif task_name == "MPCORB":
			if os.path.exists(MPCProcessorConfig.INPUT_FILE):
				print("\n[Step 1] Starting MPCORB Processing...")
				set_status("Processing MPCORB Data...")
				MPCProcessor.AsteroidProcessor(MPCProcessorConfig.INPUT_FILE, MPCProcessorConfig.OUTPUT_DIR).process()
			else:
				print(f"[ERROR] Input not found: {MPCProcessorConfig.INPUT_FILE}")

		elif task_name == "NEO":
			if os.path.exists(NEOProcessorConfig.INPUT_FILE):
				print("\n[Step 2] Starting NEO Processing...")
				set_status("Processing NEO Data...")
				NEOProcessor.AsteroidProcessor(NEOProcessorConfig.INPUT_FILE, NEOProcessorConfig.OUTPUT_DIR).process()
			else:
				print(f"[ERROR] Input not found: {NEOProcessorConfig.INPUT_FILE}")

		elif task_name == "MERGE":
			print("\n[Step 3] Starting Merge...")
			set_status("Merging Datasets...")
			DataMerger.DataMerger().run()

		elif task_name == "IMPORT":
			print("\n[Step 4] Starting DB Import...")
			set_status("Importing to Database...")
			# Patch Connection String if env var is set
			if 'SQL_CONNECTION_STRING' in os.environ:
				from importer import config as ImporterConfig
				ImporterConfig.DB_CONNECTION_STRING = os.environ.get('SQL_CONNECTION_STRING')
			DBImporter.DBImporter().run(use_bulk=use_bulk)

		elif task_name == "INIT_DB":
			print("\n[Maintenance] Initializing Database...")
			set_status("Initializing Database...")
			import scripts.init_db as init_db
			# Patch Connection String if env var is set
			if 'SQL_CONNECTION_STRING' in os.environ:
				init_db.DB_CONNECTION_STRING = os.environ.get('SQL_CONNECTION_STRING')
			init_db.run_initialization()

		elif task_name == "FULL":
			start_time = time.time()
			print("="*60)
			print("ASTEROID DATA PIPELINE AUTOMATION")
			print("="*60)

			clean_dirs()

			# Sequential Execution
			if os.path.exists(MPCProcessorConfig.INPUT_FILE):
				print("\n[Step 1] MPCORB Processing...")
				set_status("Processing MPCORB Data...")
				MPCProcessor.AsteroidProcessor(MPCProcessorConfig.INPUT_FILE, MPCProcessorConfig.OUTPUT_DIR).process()

			if os.path.exists(NEOProcessorConfig.INPUT_FILE):
				print("\n[Step 2] NEO Processing...")
				set_status("Processing NEO Data...")
				NEOProcessor.AsteroidProcessor(NEOProcessorConfig.INPUT_FILE, NEOProcessorConfig.OUTPUT_DIR).process()

			print("\n[Step 3] Merging Data...")
			set_status("Merging Datasets...")
			DataMerger.DataMerger().run()

			print("\n[Step 4] Importing to DB...")
			set_status("Importing to Database...")
			if 'SQL_CONNECTION_STRING' in os.environ:
				from importer import config as ImporterConfig
				ImporterConfig.DB_CONNECTION_STRING = os.environ.get('SQL_CONNECTION_STRING')
			DBImporter.DBImporter().run(use_bulk=use_bulk)

			end_time = time.time()
			duration = end_time - start_time
			print("\n" + "="*60)
			print(f"FULL PIPELINE COMPLETE in {duration:.2f} seconds.")
			print("="*60)

	except Exception as e:
		print(f"\n[CRITICAL ERROR] Process Failed: {e}")
		import traceback
		traceback.print_exc()

	# Sentinel value to signal end of process to GUI
	log_queue.put("___PROCESS_COMPLETE___")


# --- Main Application ---
class AsteroidPipelineApp:
	def __init__(self, root):
		self.root = root
		self.root.title("Asteroid Data Pipeline Controller")
		self.root.geometry("1150x800")

		self.colors = {
			'bg_app': '#1e1e1e',
			'bg_header': '#252526',
			'text_main': '#d4d4d4',
			'accent': '#007acc',
			'accent_hover': '#0062a3',
			'border': '#3e3e42',
			'card': '#252526',
			'success': '#238636',
			'danger': '#da3633',
			'console_bg': '#1e1e1e',
			'console_fg': '#cccccc'
		}
		self.root.configure(bg=self.colors['bg_app'])

		self.current_process = None
		self.log_queue = multiprocessing.Queue()
		self.is_running = False

		self._setup_styles()
		self._create_layout()
		self.load_env_config()
		self.root.after(100, self._poll_queue)

	def _setup_styles(self):
		style = ttk.Style()
		style.theme_use('clam')

		style.configure(".", background=self.colors['bg_app'], foreground=self.colors['text_main'], font=("Segoe UI", 10))
		style.configure("TFrame", background=self.colors['bg_app'])

		style.configure("Card.TLabelframe", background=self.colors['card'], relief="solid", borderwidth=1, bordercolor=self.colors['border'])
		style.configure("Card.TLabelframe.Label", background=self.colors['card'], foreground=self.colors['text_main'], font=("Segoe UI", 10, "bold"))

		style.configure("Primary.TButton", background=self.colors['success'], foreground="#ffffff", font=("Segoe UI", 11, "bold"), borderwidth=0)
		style.map("Primary.TButton",
				  background=[('active', '#2ea043'), ('disabled', '#238636')],
				  foreground=[('active', '#ffffff'), ('disabled', '#aaaaaa')])

		style.configure("Action.TButton", background=self.colors['accent'], foreground="#ffffff", font=("Segoe UI", 10, "bold"), borderwidth=0)
		style.map("Action.TButton",
				  background=[('active', self.colors['accent_hover']), ('disabled', '#007acc')],
				  foreground=[('active', '#ffffff'), ('disabled', '#aaaaaa')])

		style.configure("Danger.TButton", background=self.colors['danger'], foreground="#ffffff", font=("Segoe UI", 10, "bold"))
		style.map("Danger.TButton",
				  background=[('active', '#f85149')],
				  foreground=[('active', '#ffffff'), ('disabled', '#aaaaaa')])

		style.configure("TCheckbutton", background=self.colors['card'], foreground=self.colors['text_main'], font=("Segoe UI", 10))
		style.map("TCheckbutton", background=[('active', self.colors['card'])])

	def _create_layout(self):
		header = tk.Frame(self.root, bg=self.colors['bg_header'], height=70, highlightthickness=1, highlightbackground=self.colors['border'])
		header.pack(fill=tk.X)
		tk.Label(header, text="Asteroid Data Pipeline", font=("Segoe UI", 20, "bold"), bg=self.colors['bg_header'], fg="white").pack(side=tk.LEFT, padx=30, pady=15)

		main_container = tk.Frame(self.root, bg=self.colors['bg_app'])
		main_container.pack(fill=tk.BOTH, expand=True, padx=30, pady=20)

		left_col = tk.Frame(main_container, bg=self.colors['bg_app'], width=400)
		left_col.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 20))

		action_frame = ttk.LabelFrame(left_col, text=" Pipeline Actions ", style="Card.TLabelframe", padding=15)
		action_frame.pack(fill=tk.X, pady=(0, 15))

		self.btn_run_all = ttk.Button(action_frame, text="▶ START FULL PIPELINE", style="Primary.TButton", command=lambda: self.start_process("FULL"))
		self.btn_run_all.pack(fill=tk.X, pady=(0, 15))

		step_frame = tk.Frame(action_frame, bg=self.colors['card'])
		step_frame.pack(fill=tk.X)
		self.btn_step1 = ttk.Button(step_frame, text="1. Process MPCORB", style="Action.TButton", command=lambda: self.start_process("MPCORB")); self.btn_step1.pack(fill=tk.X, pady=2)
		self.btn_step2 = ttk.Button(step_frame, text="2. Process NEO", style="Action.TButton", command=lambda: self.start_process("NEO")); self.btn_step2.pack(fill=tk.X, pady=2)
		self.btn_step3 = ttk.Button(step_frame, text="3. Merge Data", style="Action.TButton", command=lambda: self.start_process("MERGE")); self.btn_step3.pack(fill=tk.X, pady=2)
		self.btn_step4 = ttk.Button(step_frame, text="4. Import DB", style="Action.TButton", command=lambda: self.start_process("IMPORT")); self.btn_step4.pack(fill=tk.X, pady=2)

		self.use_bulk_var = tk.BooleanVar(value=True)
		ttk.Checkbutton(action_frame, text="Use BULK INSERT (Faster)", variable=self.use_bulk_var).pack(pady=(10,0))

		maint_frame = ttk.LabelFrame(left_col, text=" Maintenance ", style="Card.TLabelframe", padding=15)
		maint_frame.pack(fill=tk.X, pady=(0, 15))
		self.btn_clean = ttk.Button(maint_frame, text="🗑 Clean Output Directories", style="Danger.TButton", command=lambda: self.start_process("CLEAN")); self.btn_clean.pack(fill=tk.X, pady=2)
		self.btn_init_db = ttk.Button(maint_frame, text="⚠ Initialize Database", style="Danger.TButton", command=lambda: self.start_process("INIT_DB")); self.btn_init_db.pack(fill=tk.X, pady=2)

		config_frame = ttk.LabelFrame(left_col, text=" Configuration (.env) ", style="Card.TLabelframe", padding=15)
		config_frame.pack(fill=tk.BOTH, expand=True)
		self.txt_config = scrolledtext.ScrolledText(config_frame, height=8, font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4", insertbackground="white", relief="flat", padx=5, pady=5)
		self.txt_config.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

		btn_row = tk.Frame(config_frame, bg=self.colors['card'])
		btn_row.pack(fill=tk.X)
		ttk.Button(btn_row, text="💾 Save Changes", style="Action.TButton", command=self.save_env_config).pack(side=tk.RIGHT)
		ttk.Button(btn_row, text="↻ Reload", command=self.load_env_config).pack(side=tk.RIGHT, padx=5)

		right_col = ttk.LabelFrame(main_container, text=" System Console ", style="Card.TLabelframe", padding=10)
		right_col.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
		self.txt_log = scrolledtext.ScrolledText(right_col, state='disabled', font=("Consolas", 10), bg=self.colors['console_bg'], fg=self.colors['console_fg'], insertbackground="white", relief="flat")
		self.txt_log.pack(fill=tk.BOTH, expand=True)

		self.status_var = tk.StringVar(value="Ready")
		status_bar = tk.Frame(self.root, bg=self.colors['accent'], height=30)
		status_bar.pack(fill=tk.X, side=tk.BOTTOM)
		tk.Label(status_bar, textvariable=self.status_var, bg=self.colors['accent'], fg="white", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=15)
		self.progress_bar = ttk.Progressbar(status_bar, mode='indeterminate', length=200); self.progress_bar.pack(side=tk.RIGHT, padx=15, pady=5)

	def _poll_queue(self):
		"""
		Polls the multiprocessing queue for messages.
		This is the Main Thread side of the communication.
		"""
		try:
			# Fetch up to 500 messages to update UI without freezing
			messages = []
			while not self.log_queue.empty():
				try:
					msg = self.log_queue.get_nowait()
					if msg == "___PROCESS_COMPLETE___":
						self._on_process_finished()
						continue
					messages.append(msg)
					if len(messages) > 500: break
				except queue.Empty:
					break

			if messages:
				self._update_log_window(messages)

		except Exception as e:
			print(f"Polling Error: {e}")

		# Check process status
		if self.current_process and not self.current_process.is_alive():
			if self.is_running: # If logic says running but process died unexpectedly
				self._on_process_finished()

		# Schedule next poll
		self.root.after(50, self._poll_queue)

	def _update_log_window(self, messages):
		console_buffer = []
		last_status_update = None

		for msg in messages:
			# Check for progress indicators (carriage returns or specific keywords)
			if '\r' in msg or ("Processed" in msg and "records" in msg):
				# This is a progress update - clean it for status bar
				clean = msg.replace('\r', '').strip()
				if clean:
					last_status_update = clean
			else:
				# This is a regular log message.
				# Remove trailing whitespace/newlines from the raw message
				# to prevent double-spacing, then manually add ONE newline.
				cleaned_msg = msg.rstrip()
				if cleaned_msg:
					console_buffer.append(cleaned_msg + "\n")

		# 1. Update Text Area
		if console_buffer:
			self.txt_log.configure(state='normal')
			self.txt_log.insert(tk.END, "".join(console_buffer))

			# Truncate history
			if float(self.txt_log.index('end')) > 5000:
				self.txt_log.delete('1.0', '1000.0')

			self.txt_log.see(tk.END)
			self.txt_log.configure(state='disabled')

		# 2. Update Status Bar
		if last_status_update:
			self.status_var.set(f"⚙️ {last_status_update}")

	def log_message(self, message):
		self._update_log_window([message])

	def _disable_controls(self):
		state = 'disabled'
		self.btn_run_all.configure(state=state)
		self.btn_step1.configure(state=state)
		self.btn_step2.configure(state=state)
		self.btn_step3.configure(state=state)
		self.btn_step4.configure(state=state)
		self.btn_clean.configure(state=state)
		self.btn_init_db.configure(state=state)

	def _enable_controls(self):
		state = 'normal'
		self.btn_run_all.configure(state=state)
		self.btn_step1.configure(state=state)
		self.btn_step2.configure(state=state)
		self.btn_step3.configure(state=state)
		self.btn_step4.configure(state=state)
		self.btn_clean.configure(state=state)
		self.btn_init_db.configure(state=state)

	# --- Config ---
	def load_env_config(self):
		env_path = os.path.join(PROJECT_ROOT, '.env')
		if os.path.exists(env_path):
			with open(env_path, 'r') as f:
				content = f.read()
			self.txt_config.delete("1.0", tk.END)
			self.txt_config.insert("1.0", content)
			self.log_message(f"[INFO] Loaded .env configuration from {env_path}")
		else:
			self.log_message(f"[WARN] .env file not found at {env_path}")

	def save_env_config(self):
		env_path = os.path.join(PROJECT_ROOT, '.env')
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
	def start_process(self, task_name):
		if self.is_running: return

		self.is_running = True
		self._disable_controls()
		self.status_var.set(f"⏳ Running: {task_name}...")
		self.progress_bar.start(10)

		# Prepare Env
		current_env = os.environ.copy()
		use_bulk = self.use_bulk_var.get()

		# Spawn Process
		self.current_process = multiprocessing.Process(
			target=run_worker_process,
			args=(task_name, self.log_queue, current_env, use_bulk)
		)
		self.current_process.start()

	def _on_process_finished(self):
		self.is_running = False
		self._enable_controls()
		self.status_var.set("✅ Ready")
		self.progress_bar.stop()
		self.log_message("\n=== Process Completed ===\n")
		self.current_process = None

if __name__ == "__main__":
	# Crucial for Windows Multiprocessing
	multiprocessing.freeze_support()

	if sys.stdout is None:
		sys.stdout = open(os.devnull, 'w')
	if sys.stderr is None:
		sys.stderr = open(os.devnull, 'w')

	root = tk.Tk()
	app = AsteroidPipelineApp(root)
	root.mainloop()