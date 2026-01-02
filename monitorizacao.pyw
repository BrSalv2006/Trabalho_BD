import tkinter as tk
from tkinter import ttk, messagebox
import os
import textwrap
from dotenv import load_dotenv
import mssql_python
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

# Load environment variables
load_dotenv()

COLORS = {
	'bg_app': '#1e1e1e',
	'bg_header': '#252526',
	'text_main': '#d4d4d4',
	'text_light': '#858585',
	'accent': '#007acc',
	'border': '#3e3e42',
	'card': '#252526',
	'danger': '#f85149',
	'success': '#238636'
}

class AppMonitorizacao:
	def __init__(self, root):
		self.root = root
		self.conn = None
		self.cursor = None
		self._setup_window()
		if self._connect_db():
			self._setup_styles()
			self._build_layout()
			self._init_charts()
			self.refresh_dashboard()
			self.root.after(500, self.refresh_dashboard)
		else:
			self.root.destroy()

	def _setup_window(self):
		self.root.title("Dashboard de Monitorização NEO")
		self.root.geometry("1280x800")
		self.root.configure(bg=COLORS['bg_app'])
		try:
			self.root.state('zoomed')
		except tk.TclError:
			pass

	def _connect_db(self):
		print("Connecting to DB...")
		try:
			conn_str = os.getenv('SQL_CONNECTION_STRING')
			if not conn_str:
				print("SQL_CONNECTION_STRING not found")
				raise ValueError("A variável de ambiente SQL_CONNECTION_STRING não está definida.")
			self.conn = mssql_python.connect(conn_str)
			self.cursor = self.conn.cursor()
			print("Connected!")
			return True
		except Exception as e:
			print(f"Connection Error: {e}")
			messagebox.showerror("Erro de Conexão", f"Não foi possível conectar à base de dados:\n{e}")
			return False

	def _setup_styles(self):
		style = ttk.Style()
		style.theme_use('clam')
		style.configure("TNotebook", background=COLORS['bg_app'], borderwidth=0)
		style.configure("TNotebook.Tab", background="#2d2d30", foreground="#cccccc", padding=[20, 10], font=("Segoe UI", 10))
		style.map("TNotebook.Tab", background=[("selected", COLORS['bg_header'])], foreground=[("selected", COLORS['accent'])])
		style.configure("Treeview", background=COLORS['bg_app'], foreground=COLORS['text_main'], fieldbackground=COLORS['bg_app'], rowheight=35, borderwidth=0, font=("Segoe UI", 10))
		style.configure("Treeview.Heading", background="#333333", foreground=COLORS['text_main'], font=("Segoe UI", 9, "bold"), relief="flat")

	def _build_layout(self):
		# Header
		header = tk.Frame(self.root, bg=COLORS['bg_header'], height=80, highlightthickness=1, highlightbackground=COLORS['border'])
		header.pack(fill="x")
		tk.Label(header, text="Monitorização NEO | Dashboard", font=("Segoe UI", 20, "bold"), bg=COLORS['bg_header'], fg="white").pack(side="left", padx=30, pady=20)
		tk.Button(header, text="↻ Atualizar", command=self.refresh_dashboard, bg=COLORS['card'], fg="white", font=("Segoe UI", 9), relief="solid", bd=0, padx=15, pady=5).pack(side="right", padx=30)

		# Notebook
		self.notebook = ttk.Notebook(self.root)
		self.notebook.pack(fill="both", expand=True, padx=30, pady=20)
		self.tab_stats = tk.Frame(self.notebook, bg=COLORS['bg_app'])
		self.tab_data = tk.Frame(self.notebook, bg=COLORS['bg_app'])
		self.notebook.add(self.tab_stats, text="📊 Visão Geral")
		self.notebook.add(self.tab_data, text="📋 Eventos Críticos")

		self._build_tab_stats()
		self._build_tab_data()

	def _build_tab_stats(self):
		# KPIs
		kpi_frame = tk.Frame(self.tab_stats, bg=COLORS['bg_app'])
		kpi_frame.pack(fill="x", pady=(20, 20))
		self.lbl_total = self._create_kpi_card(kpi_frame, "Total Asteroides", COLORS['accent'])
		self.lbl_neos = self._create_kpi_card(kpi_frame, "NEOs Identificados", COLORS['success'])
		self.lbl_phas = self._create_kpi_card(kpi_frame, "PHAs (Perigosos)", COLORS['danger'])
		self.lbl_new = self._create_kpi_card(kpi_frame, "Descobertas (Mês)", COLORS['text_light'])

		# Charts Container
		charts_frame = tk.Frame(self.tab_stats, bg=COLORS['bg_app'])
		charts_frame.pack(fill="both", expand=True)
		self.frame_chart_classes = tk.Frame(charts_frame, bg=COLORS['card'], highlightbackground=COLORS['border'], highlightthickness=1)
		self.frame_chart_classes.pack(side="left", fill="both", expand=True, padx=(0, 15))
		self.frame_chart_precision = tk.Frame(charts_frame, bg=COLORS['card'], highlightbackground=COLORS['border'], highlightthickness=1)
		self.frame_chart_precision.pack(side="right", fill="both", expand=True)

	def _build_tab_data(self):
		container = tk.Frame(self.tab_data, bg=COLORS['card'], highlightbackground=COLORS['border'], highlightthickness=1)
		container.pack(fill="both", expand=True, pady=20)
		tk.Label(container, text="Top 5 - Aproximações Críticas", font=("Segoe UI", 14, "bold"), bg=COLORS['card'], fg="white").pack(anchor="w", padx=20, pady=15)

		cols = ("Data", "Nome", "Designação", "Distância", "Diâmetro")
		self.tree = ttk.Treeview(container, columns=cols, show="headings")
		headers = [("Data", "Data"), ("Nome", "Nome"), ("Designação", "Designação Provisória"), ("Distância", "Distância (LD)"), ("Diâmetro", "Diâmetro (km)")]
		for col, text in headers:
			self.tree.heading(col, text=text)
			self.tree.column(col, anchor="center")

		sb = ttk.Scrollbar(container, orient="vertical", command=self.tree.yview)
		self.tree.configure(yscroll=sb.set)
		self.tree.pack(side="left", fill="both", expand=True, padx=1, pady=1)
		sb.pack(side="right", fill="y", padx=1, pady=1)
		self.tree.tag_configure('critico', background='#4a1818', foreground='#ff9999')

	def _create_kpi_card(self, parent, title, color):
		card = tk.Frame(parent, bg=COLORS['card'], highlightbackground=COLORS['border'], highlightthickness=1, padx=20, pady=15)
		card.pack(side="left", fill="x", expand=True, padx=10)
		tk.Frame(card, bg=color, width=4).pack(side="left", fill="y", padx=(0, 15))
		content = tk.Frame(card, bg=COLORS['card'])
		content.pack(side="left")
		lbl_val = tk.Label(content, text="0", font=("Segoe UI", 24, "bold"), bg=COLORS['card'], fg="white")
		lbl_val.pack(anchor="w")
		tk.Label(content, text=title.upper(), font=("Segoe UI", 9, "bold"), bg=COLORS['card'], fg=COLORS['text_light']).pack(anchor="w")
		return lbl_val

	def _init_charts(self):
		# Chart 1: Classes
		self.fig1 = Figure(figsize=(5, 4), dpi=100)
		self.fig1.patch.set_facecolor(COLORS['card'])
		self.ax1 = self.fig1.add_subplot(111)
		self.ax1.set_facecolor(COLORS['bg_app'])
		self.canvas1 = FigureCanvasTkAgg(self.fig1, self.frame_chart_classes)
		self.canvas1.get_tk_widget().pack(fill="both", expand=True)

		# Chart 2: Precision
		self.fig2 = Figure(figsize=(5, 4), dpi=100)
		self.fig2.patch.set_facecolor(COLORS['card'])
		self.ax2 = self.fig2.add_subplot(111)
		self.ax2.set_facecolor(COLORS['bg_app'])
		self.canvas2 = FigureCanvasTkAgg(self.fig2, self.frame_chart_precision)
		self.canvas2.get_tk_widget().pack(fill="both", expand=True)

	def refresh_dashboard(self):
		self._update_kpis()
		self._update_charts()
		self._update_treeview()

	def _update_kpis(self):
		queries = [
			("SELECT COUNT(*) FROM Asteroide", self.lbl_total),
			("SELECT COUNT(*) FROM Asteroide WHERE neo=1", self.lbl_neos),
			("SELECT COUNT(*) FROM Asteroide WHERE pha=1", self.lbl_phas),
			("SELECT * FROM vw_EstatisticasDescoberta", self.lbl_new)
		]
		for query, label in queries:
			try:
				self.cursor.execute(query)
				val = self.cursor.fetchone()
				text = f"{val[0]:,}" if val else "0"
				label.config(text=text)
			except Exception as e:
				print(f"Erro KPI ({query}): {e}")
				label.config(text="Err")

	def _update_charts(self):
		self.ax1.clear()
		try:
			self.cursor.execute("""
				SELECT TOP 5 c.Descricao, COUNT(*)
				FROM Orbita o
				JOIN Classe c ON o.IDClasse = c.IDClasse
				GROUP BY c.Descricao
				ORDER BY COUNT(*) DESC
			""")
			data = self.cursor.fetchall()
			if data:
				labels = ['\n'.join(textwrap.wrap(r[0], 25)) for r in data][::-1]
				values = [r[1] for r in data][::-1]
				bars = self.ax1.barh(labels, values, color=COLORS['accent'])
				self.ax1.bar_label(bars, color='white', padding=5, fmt='%d', fontweight='bold')
				self.ax1.set_xlim(right=max(values) * 1.2)
				self.ax1.set_title("Distribuição por Classe (Top 5)", fontsize=10, fontweight='bold', color='white', pad=10)
				self.ax1.xaxis.set_major_formatter(FuncFormatter(self._format_thousands))

				# Align y-labels to the right
				self.ax1.set_yticks(range(len(labels)))
				self.ax1.set_yticklabels(labels, ha='right')

				self._style_axes(self.ax1)
				self.fig1.tight_layout(pad=1)
				self.canvas1.draw()
		except Exception as e:
			print(f"Erro Chart 1: {e}")

		# Update Chart 2
		self.ax2.clear()
		try:
			self.cursor.execute("SELECT * FROM vw_EvolucaoPrecisao ORDER BY Ano")
			data = self.cursor.fetchall()
			if data:
				years = [r[0] for r in data]
				values = [r[1] for r in data]
				self.ax2.plot(years, values, marker='o', color=COLORS['danger'], linewidth=2)
				self.ax2.set_title("Evolução Precisão (RMS Médio)", fontsize=10, fontweight='bold', color='white')
				self.ax2.grid(True, linestyle='--', alpha=0.1, color='white')
				self._style_axes(self.ax2)
				self.fig2.tight_layout(pad=1)
				self.canvas2.draw()
		except Exception as e:
			print(f"Erro Chart 2: {e}")

	def _update_treeview(self):
		for item in self.tree.get_children():
			self.tree.delete(item)
		try:
			query = """
				SELECT
					CONVERT(DATE, Data_Proxima_Aproximacao),
					Nome,
					Designacao_Provisoria,
					CONVERT(FLOAT, Distancia_Lunar),
					CONVERT(FLOAT, Diametro)
				FROM vw_ProximosEventosCriticos
			"""
			self.cursor.execute(query)
			for row in self.cursor.fetchall():
				self.tree.insert("", "end", values=(row[0], row[1], row[2], row[3], row[4]), tags=('critico',))
		except Exception as e:
			print(f"Erro Treeview: {e}")

	def _style_axes(self, ax):
		ax.tick_params(axis='x', colors='white', rotation=15)
		ax.tick_params(axis='y', colors='white', labelsize=8)
		ax.spines['bottom'].set_color('#555555')
		ax.spines['top'].set_visible(False)
		ax.spines['right'].set_visible(False)
		ax.spines['left'].set_color('#555555')
		ax.set_facecolor(COLORS['bg_app'])

	def _format_thousands(self, x, pos):
		if x >= 1e6: return f'{x*1e-6:1.1f}M'
		elif x >= 1e3: return f'{x*1e-3:.0f}K'
		return f'{x:.0f}'

if __name__ == "__main__":
	root = tk.Tk()
	app = AppMonitorizacao(root)
	root.mainloop()