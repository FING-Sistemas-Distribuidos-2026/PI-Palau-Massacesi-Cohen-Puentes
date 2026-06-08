"""
cli.py
------
CLI principal del Teléfono Distorsionado.
Interfaz interactiva para enviar frases al sistema y consultar estado.
Conecta a la API real en http://localhost:8000
"""

import time
import requests
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.live import Live
from rich.spinner import Spinner
from rich.columns import Columns
from rich import box

console = Console()
LAST_SENT_JOB_ID: str | None = None

# API Configuration
API_URL = "http://localhost:8000"
# API_URL = "http://api:8000"
REQUEST_TIMEOUT = 5

# Error messages
ERROR_API_UNAVAILABLE = "[red]✗ API no disponible. ¿Está levantada?[/red]"
ERROR_NETWORK = "[red]✗ Error de conexión con la API[/red]"


def check_api_health():
    """Verify API is available"""
    try:
        resp = requests.get(f"{API_URL}/health", timeout=REQUEST_TIMEOUT)
        return resp.status_code == 200
    except:
        return False


def send_phrase_to_api(phrase: str, num_workers: int) -> dict | None:
    """Send phrase to API and get job ID"""
    try:
        resp = requests.post(
            f"{API_URL}/send",
            params={"phrase": phrase, "num_workers": num_workers},
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            console.print(f"  [red]✗ Error: {resp.status_code}[/red]")
            return None
    except requests.exceptions.ConnectionError:
        console.print(ERROR_NETWORK)
        return None
    except Exception as e:
        console.print(f"  [red]✗ Error: {str(e)}[/red]")
        return None


def get_job_status(job_id: str) -> dict | None:
    """Get job status from API"""
    try:
        resp = requests.get(
            f"{API_URL}/job/{job_id}/status",
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return None
        else:
            console.print(f"  [red]✗ Error: {resp.status_code}[/red]")
            return None
    except requests.exceptions.ConnectionError:
        console.print(ERROR_NETWORK)
        return None
    except Exception as e:
        console.print(f"  [red]✗ Error: {str(e)}[/red]")
        return None


def get_all_jobs() -> list | None:
    """Get all jobs from API"""
    try:
        resp = requests.get(
            f"{API_URL}/jobs",
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("jobs", [])
        else:
            console.print(f"  [red]✗ Error: {resp.status_code}[/red]")
            return None
    except requests.exceptions.ConnectionError:
        console.print(ERROR_NETWORK)
        return None
    except Exception as e:
        console.print(f"  [red]✗ Error: {str(e)}[/red]")
        return None


def get_distortions(job_id: str) -> list | None:
    """Get distorted phrases for a job"""
    try:
        resp = requests.get(
            f"{API_URL}/job/{job_id}/distortions",
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("distortions", [])
        elif resp.status_code == 404:
            return None
        else:
            return []
    except:
        return []


def get_guesses(job_id: str) -> list | None:
    """Get guesses (final results) for a job"""
    try:
        resp = requests.get(
            f"{API_URL}/job/{job_id}/guesses",
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("guesses", [])
        elif resp.status_code == 404:
            return None
        else:
            return []
    except:
        return []


def print_header():
    console.clear()
    console.print(Panel(
        Text(" TELÉFONO RUIDOSO", style="bold white", justify="center"),
        subtitle="[dim]sistema distribuido de ruido lingüístico[/dim]",
        style="bold cyan",
        border_style="cyan",
        padding=(1, 4),
    ))
    console.print()


def check_api_before_action():
    """Check if API is available before performing actions"""
    if not check_api_health():
        console.print(f"  {ERROR_API_UNAVAILABLE}\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return False
    return True


def sort_jobs_by_newest(jobs: list[dict]) -> list[dict]:
    """Order jobs from newest to oldest using the API timestamp."""
    return sorted(
        jobs,
        key=lambda job: (
            job.get("job_id") == LAST_SENT_JOB_ID,
            job.get("created_at", ""),
            job.get("job_id", ""),
        ),
        reverse=True,
    )


def print_menu():
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column(style="bold cyan", width=4)
    table.add_column(style="white")
    table.add_row("[1]", "Enviar frase al sistema")
    table.add_row("[2]", "Ver estado de las frases")
    table.add_row("[3]", "Ver detalle de una frase")
    table.add_row("[4]", "Ver adivinanzas de LLM")
    table.add_row("[0]", "Salir")
    console.print(Panel(table, title="[bold]MENÚ[/bold]", border_style="dim white", padding=(0, 1)))
    console.print()


def send_phrase():
    if not check_api_before_action():
        return
        
    console.print(Panel("[bold cyan]NUEVA FRASE[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    frase = Prompt.ask("[bold]  Frase[/bold]")
    if not frase.strip():
        console.print("  [red]✗ La frase no puede estar vacía.[/red]\n")
        return

    copias = IntPrompt.ask("[bold]  Cantidad de copias[/bold]", default=3)
    if copias < 1 or copias > 2000:
        console.print("  [red]✗ La cantidad de copias debe estar entre 1 y 2000.[/red]\n")
        return

    console.print()

    # Confirmación
    table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2), border_style="dim")
    table.add_column(style="dim", width=12)
    table.add_column(style="white")
    table.add_row("frase", f'"{frase}"')
    table.add_row("copias", str(copias))
    console.print(table)
    console.print()

    if not Confirm.ask("  [bold]¿Enviar?[/bold]", default=True):
        console.print("  [dim]Cancelado.[/dim]\n")
        return

    # Enviar a la API
    console.print()
    with console.status("[cyan]Enviando al sistema...[/cyan]", spinner="dots"):
        result = send_phrase_to_api(frase, copias)

    if result:
        global LAST_SENT_JOB_ID
        frase_id = result.get("job_id")
        LAST_SENT_JOB_ID = frase_id
        console.print(f"  [bold green]✓ Frase enviada[/bold green]  [dim]{frase_id}[/dim]")
    else:
        console.print("  [red]✗ Error al enviar la frase[/red]")
    
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")


def list_jobs():
    if not check_api_before_action():
        return
        
    console.print(Panel("[bold cyan]ESTADO DE FRASES[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    with console.status("[cyan]Obteniendo frases...[/cyan]", spinner="dots"):
        jobs = get_all_jobs()

    if jobs is None:
        console.print("  [red]✗ Error al obtener las frases[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    jobs = sort_jobs_by_newest(jobs)
        
    if not jobs:
        console.print("  [dim]No hay frases aún.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    table = Table(box=box.ROUNDED, border_style="dim", padding=(0, 1))
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True, width=36)
    table.add_column("Frase", style="white", max_width=30)
    table.add_column("Copias", justify="center")
    table.add_column("Avance", justify="center")
    table.add_column("Estado", justify="center")

    selectable_jobs = []

    for index, job in enumerate(jobs, start=1):
        job_id = job["job_id"]
        phrase = job["phrase"]
        num_workers = job["num_workers"]
        status = job["status"]
        
        completed = job.get("completed_workers", 0)
        bar = "█" * completed + "░" * (num_workers - completed)
        
        estado = "[green]✓ lista[/green]" if status == "completed" else "[yellow]⟳ en proceso[/yellow]"
        accion = "[bold green][ver][/bold green]" if status == "completed" else "[dim]—[/dim]"

        table.add_row(
            str(index),
            job_id,
            f'"{phrase[:28]}{"…" if len(phrase) > 28 else ""}"',
            str(num_workers),
            f"[cyan]{bar}[/cyan] {completed}/{num_workers}",
            estado,
        )
        selectable_jobs.append(job)

    console.print(table)
    console.print("  [dim]Escribe el número de una frase para ver sus distorsiones, o Enter para volver.[/dim]")
    console.print()

    selection = Prompt.ask("  [bold cyan]>[/bold cyan]", default="")
    if not selection.strip():
        return

    if not selection.isdigit():
        console.print("  [red]✗ Debes escribir un número de la lista.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    selected_index = int(selection)
    if selected_index < 1 or selected_index > len(selectable_jobs):
        console.print("  [red]✗ Número fuera de rango.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    selected_job = selectable_jobs[selected_index - 1]
    show_distortions_for_job(selected_job["job_id"], selected_job["phrase"])


def show_llm_guesses_for_job(job_id: str, phrase: str | None = None):
    if not check_api_before_action():
        return

    console.print(Panel("[bold cyan]ADIVINANZAS DE LLM[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    if phrase:
        console.print(f"  [dim]{phrase}[/dim]\n")

    with console.status("[cyan]Buscando adivinanzas...[/cyan]", spinner="dots"):
        guesses = get_guesses(job_id)

    if guesses is None:
        console.print(f"  [red]✗ Frase '{job_id}' no encontrada.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    if not guesses:
        console.print("  [dim]Todavía no hay adivinanzas guardadas para esta frase.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    table = Table(box=box.ROUNDED, border_style="dim", padding=(0, 1))
    table.add_column("Frase original", style="white", max_width=40)
    table.add_column("Adivinanza del LLM", style="cyan")

    original_phrase = phrase or guesses[0].get("phrase", "")
    for guess in guesses:
        table.add_row(
            f'"{original_phrase}"',
            f'"{guess.get("guess", "")}"',
        )

    console.print(table)
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")


def show_llm_guesses_menu():
    if not check_api_before_action():
        return

    console.print(Panel("[bold cyan]ADIVINANZAS DE LLM[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    with console.status("[cyan]Obteniendo frases...[/cyan]", spinner="dots"):
        jobs = get_all_jobs()

    if jobs is None:
        console.print("  [red]✗ Error al obtener las frases[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    jobs = sort_jobs_by_newest(jobs)

    if not jobs:
        console.print("  [dim]No hay frases aún.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    table = Table(box=box.ROUNDED, border_style="dim", padding=(0, 1))
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True, width=36)
    table.add_column("Frase", style="white", max_width=40)
    table.add_column("Estado", justify="center")

    selectable_jobs = []

    for index, job in enumerate(jobs, start=1):
        job_id = job["job_id"]
        phrase = job["phrase"]
        status = job["status"]
        estado = "[green]✓ lista[/green]" if status == "completed" else "[yellow]⟳ en proceso[/yellow]"

        table.add_row(
            str(index),
            job_id,
            f'"{phrase[:38]}{"…" if len(phrase) > 38 else ""}"',
            estado,
        )
        selectable_jobs.append(job)

    console.print(table)
    console.print("  [dim]Escribe el número de una frase para ver la adivinanza del LLM, o Enter para volver.[/dim]")
    console.print()

    selection = Prompt.ask("  [bold cyan]>[/bold cyan]", default="")
    if not selection.strip():
        return

    if not selection.isdigit():
        console.print("  [red]✗ Debes escribir un número de la lista.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    selected_index = int(selection)
    if selected_index < 1 or selected_index > len(selectable_jobs):
        console.print("  [red]✗ Número fuera de rango.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    selected_job = selectable_jobs[selected_index - 1]
    show_llm_guesses_for_job(selected_job["job_id"], selected_job["phrase"])


def job_detail():
    if not check_api_before_action():
        return
        
    console.print(Panel("[bold cyan]DETALLE DE FRASE[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    job_id = Prompt.ask("  [bold]ID de la frase[/bold]")
    if not job_id.strip():
        console.print("  [red]✗ ID no puede estar vacío[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    with console.status("[cyan]Obteniendo detalles...[/cyan]", spinner="dots"):
        status_data = get_job_status(job_id)
        distortions = get_distortions(job_id) if status_data else None
        guesses = get_guesses(job_id) if status_data else None

    if status_data is None:
        console.print(f"  [red]✗ Frase '{job_id}' no encontrada.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    console.print()
    # Status table
    table = Table(box=box.ROUNDED, show_header=False, border_style="dim", padding=(0, 2))
    table.add_column(style="dim", width=18)
    table.add_column(style="white")

    completed = status_data.get("completed_workers", 0)
    total = status_data.get("num_workers", 0)
    bar = "█" * completed + "░" * (total - completed) if total > 0 else "—"
    estado_str = "[green]✓ lista[/green]" if status_data["status"] == "completed" else "[yellow]⟳ en proceso[/yellow]"

    table.add_row("[bold]frase id[/bold]", job_id)
    table.add_row("[bold]frase original[/bold]", f'"{status_data["phrase"]}"')
    table.add_row("[bold]copias[/bold]", str(total))
    table.add_row("[bold]avance[/bold]", f"[cyan]{bar}[/cyan]  {completed}/{total}")
    table.add_row("[bold]estado[/bold]", estado_str)
    table.add_row("[bold]creado[/bold]", status_data.get("created_at", "—"))

    console.print(table)
    console.print()

    # Distortions
    if distortions and len(distortions) > 0:
        console.print("[bold cyan]Frases distorsionadas:[/bold cyan]")
        dist_table = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
        dist_table.add_column("Copia", style="dim")
        dist_table.add_column("Frase distorsionada")
        for d in distortions:
            dist_table.add_row(str(d.get("worker_id", "?")), f'"{d.get("distorted_phrase", "")}"')
        console.print(dist_table)
        console.print()

    # Guesses
    if guesses and len(guesses) > 0:
        console.print("[bold cyan]Adivinanzas (resultados):[/bold cyan]")
        guess_table = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
        guess_table.add_column("Batch", style="dim")
        guess_table.add_column("Adivinanza")
        for g in guesses:
            guess_table.add_row(str(g.get("batch_num", "?")), f'"{g.get("guess", "")}"')
        console.print(guess_table)
        console.print()
    elif status_data["status"] == "completed" and not guesses:
        console.print("  [dim]Adivinanzas: aún no disponibles[/dim]\n")
    
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")


def show_distortions_for_job(job_id: str, phrase: str | None = None):
    if not check_api_before_action():
        return

    console.print(Panel("[bold cyan]DISTORSIONES GUARDADAS[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    if phrase:
        console.print(f"  [dim]{phrase}[/dim]\n")

    with console.status("[cyan]Buscando distorsiones...[/cyan]", spinner="dots"):
        distortions = get_distortions(job_id)

    if distortions is None:
        console.print(f"  [red]✗ Frase '{job_id}' no encontrada.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    if not distortions:
        console.print("  [dim]Todavía no hay distorsiones guardadas para esta frase.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    table = Table(box=box.ROUNDED, border_style="dim", padding=(0, 1))
    table.add_column("Copia", style="dim", width=8)
    table.add_column("Frase distorsionada", style="white")

    for d in distortions:
        table.add_row(str(d.get("worker_id", "?")), f'"{d.get("distorted_phrase", "")}"')

    console.print(table)
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")



def main():
    while True:
        print_header()
        print_menu()
        choice = Prompt.ask("  [bold cyan]>[/bold cyan]", choices=["0", "1", "2", "3", "4"], show_choices=False)

        if choice == "1":
            print_header()
            send_phrase()
        elif choice == "2":
            print_header()
            list_jobs()
        elif choice == "3":
            print_header()
            job_detail()
        elif choice == "4":
            print_header()
            show_llm_guesses_menu()
        elif choice == "0":
            console.print("\n  [dim]Hasta luego.[/dim]\n")
            break


if __name__ == "__main__":
    main()