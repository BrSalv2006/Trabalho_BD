import tkinter as tk
from tkinter import ttk, messagebox
import mssql_python
import os
from dotenv import load_dotenv
import threading
import queue

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
    'success': '#238636',
    'input_bg': '#3c3c3c',
    'input_fg': '#ffffff'
}

class AppAlertas:
    def __init__(self, root):
        self.root = root
        self.root.title("Gestão de Alertas NEO")
        self.root.geometry("1200x750")
        self.root.configure(bg=COLORS['bg_app'])
        self.notificacoes_ativas = tk.BooleanVar(value=True)

        self.data_queue = queue.Queue()
        self.loading = False
        self.conn = None
        self.cursor = None

        if not self.ligar_bd(): return

        self.configurar_estilos()
        self.construir_layout()

        # Start polling the queue for background updates
        self.check_queue()

        # Initial load
        self.carregar_alertas()

        # Ensure connection closes on exit
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def ligar_bd(self):
        try:
            conn_str = os.getenv('SQL_CONNECTION_STRING')
            if not conn_str:
                raise ValueError("A variável de ambiente SQL_CONNECTION_STRING não está definida no ficheiro .env")

            self.conn = mssql_python.connect(conn_str)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            messagebox.showerror("Erro Crítico", f"Falha SQL:\n{e}")
            self.root.destroy()
            return False

    def on_close(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        self.root.destroy()

    def configurar_estilos(self):
        style = ttk.Style()
        style.theme_use('clam')

        style.configure(".", background=COLORS['bg_app'], foreground=COLORS['text_main'])

        style.configure("Treeview",
                        background=COLORS['bg_app'],
                        foreground=COLORS['text_main'],
                        fieldbackground=COLORS['bg_app'],
                        rowheight=35,
                        borderwidth=0,
                        font=("Segoe UI", 10))

        style.configure("Treeview.Heading",
                        background="#333333",
                        foreground=COLORS['text_main'],
                        font=("Segoe UI", 9, "bold"),
                        relief="flat")

        style.map("Treeview",
                  background=[('selected', '#37373d')],
                  foreground=[('selected', '#ffffff')])

        style.configure("TCombobox", fieldbackground=COLORS['input_bg'], background=COLORS['input_bg'], foreground=COLORS['input_fg'], arrowcolor=COLORS['text_main'], bordercolor=COLORS['border'])
        style.map('TCombobox', fieldbackground=[('readonly', COLORS['input_bg'])], selectbackground=[('readonly', COLORS['input_bg'])], selectforeground=[('readonly', COLORS['input_fg'])])

        self.tree_tags = {
            'nivel_4': {'background': '#4a1212', 'foreground': '#ff9999'}, # Vermelho (Nível 4)
            'nivel_3': {'background': '#542e08', 'foreground': '#ffcc80'}, # Laranja (Nível 3)
            'nivel_2': {'background': '#423e0f', 'foreground': '#fff59d'}, # Amarelo (Nível 2)
            'nivel_1': {'background': '#1b3320', 'foreground': '#a5d6a7'}, # Verde (Nível 1)
            'normal':  {'background': '#1e1e1e', 'foreground': '#d4d4d4'}  # Outros
        }

    def construir_layout(self):
        header = tk.Frame(self.root, bg=COLORS['bg_header'], height=80, highlightthickness=1, highlightbackground=COLORS['border'])
        header.pack(fill="x")
        tk.Label(header, text="Gestão de Alertas NEO", font=("Segoe UI", 22, "bold"), bg=COLORS['bg_header'], fg="white").pack(side="left", padx=30, pady=20)

        chk = tk.Checkbutton(header, text="Notificações Ativas", variable=self.notificacoes_ativas, bg=COLORS['bg_header'], fg=COLORS['accent'], font=("Segoe UI", 10, "bold"), selectcolor=COLORS['bg_header'], activebackground=COLORS['bg_header'], activeforeground=COLORS['accent'])
        chk.pack(side="right", padx=30)

        toolbar = tk.Frame(self.root, bg=COLORS['bg_app'], padx=30, pady=20)
        toolbar.pack(fill="x")

        tk.Label(toolbar, text="Prioridade", font=("Segoe UI", 9, "bold"), bg=COLORS['bg_app'], fg=COLORS['text_light']).pack(side="left")
        self.combo_prio = ttk.Combobox(toolbar, values=["Todas", "Alta", "Média", "Baixa"], state="readonly", width=15)
        self.combo_prio.current(0); self.combo_prio.pack(side="left", padx=(5, 20))

        tk.Label(toolbar, text="Nível Torino", font=("Segoe UI", 9, "bold"), bg=COLORS['bg_app'], fg=COLORS['text_light']).pack(side="left")
        self.combo_nivel = ttk.Combobox(toolbar, values=["Todos", "4 - Vermelho", "3 - Laranja", "2 - Amarelo", "1 - Verde"], state="readonly", width=18)
        self.combo_nivel.current(0); self.combo_nivel.pack(side="left", padx=(5, 20))

        self.btn_atualizar = tk.Button(toolbar, text="Atualizar Lista", command=self.carregar_alertas, bg=COLORS['card'], fg=COLORS['text_main'], font=("Segoe UI", 9), relief="solid", bd=0, padx=15, pady=4, activebackground=COLORS['border'], activeforeground="white")
        self.btn_atualizar.pack(side="left")

        card_frame = tk.Frame(self.root, bg=COLORS['card'], highlightbackground=COLORS['border'], highlightthickness=1)
        card_frame.pack(fill="both", expand=True, padx=30, pady=(0, 20))

        cols = ("ID", "Data", "Asteroide", "Prioridade", "Nível", "Descrição")
        self.tree = ttk.Treeview(card_frame, columns=cols, show="headings")
        self.tree.heading("ID", text="#"); self.tree.column("ID", width=50, anchor="center")
        self.tree.heading("Data", text="Data"); self.tree.column("Data", width=100, anchor="center")
        self.tree.heading("Asteroide", text="Asteroide"); self.tree.column("Asteroide", width=200)
        self.tree.heading("Prioridade", text="Prioridade"); self.tree.column("Prioridade", width=100, anchor="center")
        self.tree.heading("Nível", text="Torino"); self.tree.column("Nível", width=80, anchor="center")
        self.tree.heading("Descrição", text="Motivo"); self.tree.column("Descrição", width=400)

        sb = ttk.Scrollbar(card_frame, orient="vertical", command=self.tree.yview); self.tree.configure(yscroll=sb.set)
        self.tree.pack(side="left", fill="both", expand=True, padx=1, pady=1); sb.pack(side="right", fill="y", padx=1, pady=1)

        for tag, cfg in self.tree_tags.items():
            self.tree.tag_configure(tag, background=cfg['background'], foreground=cfg['foreground'])

        footer = tk.Frame(self.root, bg=COLORS['bg_app'], pady=20); footer.pack(fill="x")
        tk.Button(footer, text="Arquivar Alerta Selecionado", command=self.resolver_alerta, bg=COLORS['success'], fg="white", font=("Segoe UI", 11, "bold"), relief="flat", padx=30, pady=10).pack()

    def check_queue(self):
        try:
            while True:
                action, data = self.data_queue.get_nowait()
                if action == 'update_tree':
                    self.populate_tree(data)
                elif action == 'error':
                    messagebox.showerror("Erro", data)
                elif action == 'loading_start':
                    self.btn_atualizar.config(state="disabled", text="A carregar...")
                elif action == 'loading_end':
                    self.btn_atualizar.config(state="normal", text="Atualizar Lista")
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.check_queue)

    def carregar_alertas(self):
        if self.loading: return
        self.loading = True
        self.data_queue.put(('loading_start', None))

        # Capture current filter values
        prio = self.combo_prio.get()
        nivel = self.combo_nivel.get()
        conn_str = os.getenv('SQL_CONNECTION_STRING')

        # Run in background thread
        thread = threading.Thread(target=self.fetch_data_thread, args=(conn_str, prio, nivel))
        thread.daemon = True
        thread.start()

    def fetch_data_thread(self, conn_str, prio, nivel):
        conn = None
        try:
            # Create a dedicated connection for this thread
            conn = mssql_python.connect(conn_str)
            cursor = conn.cursor()

            query = """
                SELECT
                    al.ID_Alerta,
                    al.Data_Alerta,
                    ast.name,
                    ast.pdes,
                    al.Prioridade,
                    al.Nivel,
                    CAST(al.Descricao AS VARCHAR(MAX)) as Descricao
                FROM Alerta al
                JOIN Asteroide ast ON al.IDAsteroide = ast.IDAsteroide
                WHERE al.Estado = 'Ativo'
            """
            params = []
            if prio != "Todas":
                query += " AND al.Prioridade = ?"
                params.append(prio)
            if nivel != "Todos":
                query += " AND al.Nivel = ?"
                params.append(nivel.split(" - ")[0])

            query += " ORDER BY al.Nivel DESC, al.Data_Alerta DESC"

            cursor.execute(query, params)
            rows = cursor.fetchall()
            self.data_queue.put(('update_tree', rows))

        except Exception as e:
            self.data_queue.put(('error', str(e)))
        finally:
            if conn:
                try: conn.close()
                except: pass
            self.loading = False
            self.data_queue.put(('loading_end', None))

    def populate_tree(self, rows):
        for item in self.tree.get_children(): self.tree.delete(item)
        count_alta = 0
        for row in rows:
            nome = row[2] if row[2] else row[3]

            nivel = int(row[5])
            tag = 'normal'
            if nivel == 4: tag = 'nivel_4'
            elif nivel == 3: tag = 'nivel_3'
            elif nivel == 2: tag = 'nivel_2'
            elif nivel == 1: tag = 'nivel_1'

            self.tree.insert("", "end", values=(row[0], row[1], nome, row[4], row[5], row[6]), tags=(tag,))
            if nivel >= 3: count_alta += 1

        if self.notificacoes_ativas.get() and count_alta > 0:
            self.root.title(f"⚠️ {count_alta} ALERTAS CRÍTICOS")
        else:
            self.root.title("Gestão de Alertas NEO")

    def resolver_alerta(self):
        sel = self.tree.selection()
        if not sel: messagebox.showwarning("Aviso", "Seleciona um alerta."); return
        if messagebox.askyesno("Confirmar", "Arquivar este alerta?"):
            try:
                # Use the main connection for write operations
                self.cursor.execute("{CALL sp_AlterarEstadoAlerta (?, ?)}", (self.tree.item(sel[0])['values'][0], 'Resolvido'))
                self.conn.commit()
                self.carregar_alertas()
                messagebox.showinfo("Sucesso", "Alerta arquivado.")
            except Exception as e: messagebox.showerror("Erro", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    app = AppAlertas(root)
    root.mainloop()