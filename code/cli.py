"""
cli.py
------
CLI principal del Teléfono Descompuesto.
Interfaz interactiva para enviar frases al sistema y consultar estado.
"""

import time
import random
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


# Estado simulado del sistema (luego se reemplaza por llamadas reales)

_jobs = []  # lista de jobs enviados

def _mock_job_status(job_id: str) -> dict:
    """Simula un estado de job. Reemplazar con llamada real a la API/K8s."""
    job = next((j for j in _jobs if j["id"] == job_id), None)
    if not job:
        return {}
    elapsed = time.time() - job["created_at"]
    total = job["workers"]
    done = min(int(elapsed * 1.2), total)
    status = "completado" if done >= total else "en progreso"
    return {
        **job,
        "done_workers": done,
        "status": status,
        "result": job.get("result", "—"),
    }


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


def print_menu():
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column(style="bold cyan", width=4)
    table.add_column(style="white")
    table.add_row("[1]", "Enviar frase al sistema")
    table.add_row("[2]", "Ver estado de los jobs")
    table.add_row("[3]", "Ver detalle de un job")
    table.add_row("[0]", "Salir")
    console.print(Panel(table, title="[bold]MENÚ[/bold]", border_style="dim white", padding=(0, 1)))
    console.print()


def send_phrase():
    console.print(Panel("[bold cyan]NUEVO JOB[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    frase = Prompt.ask("[bold]  Frase[/bold]")
    if not frase.strip():
        console.print("  [red]✗ La frase no puede estar vacía.[/red]\n")
        return

    workers = IntPrompt.ask("[bold]  Workers[/bold]", default=3)
    if workers < 1 or workers > 2000:
        console.print("  [red]✗ Workers debe estar entre 1 y 2000.[/red]\n")
        return

    console.print()

    # Confirmación
    table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2), border_style="dim")
    table.add_column(style="dim", width=12)
    table.add_column(style="white")
    table.add_row("frase", f'"{frase}"')
    table.add_row("workers", str(workers))
    console.print(table)
    console.print()

    if not Confirm.ask("  [bold]¿Enviar?[/bold]", default=True):
        console.print("  [dim]Cancelado.[/dim]\n")
        return

    # Simular envío
    console.print()
    with console.status("[cyan]Enviando al sistema...[/cyan]", spinner="dots"):
        time.sleep(1.2)

    job_id = f"job-{random.randint(1000,9999)}"
    _jobs.append({
        "id": job_id,
        "frase": frase,
        "workers": workers,
        "created_at": time.time(),
        "created_str": datetime.now().strftime("%H:%M:%S"),
    })

    console.print(f"  [bold green]✓ Job enviado[/bold green]  [dim]{job_id}[/dim]")
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")


def list_jobs():
    console.print(Panel("[bold cyan]ESTADO DE JOBS[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    if not _jobs:
        console.print("  [dim]No hay jobs enviados aún.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    table = Table(box=box.ROUNDED, border_style="dim", padding=(0, 1))
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Frase", style="white", max_width=35)
    table.add_column("Workers", justify="center")
    table.add_column("Progreso", justify="center")
    table.add_column("Estado", justify="center")
    table.add_column("Hora", style="dim")

    for job in reversed(_jobs):
        s = _mock_job_status(job["id"])
        done = s["done_workers"]
        total = s["workers"]
        bar = "█" * done + "░" * (total - done)

        if s["status"] == "completado":
            estado = "[green]✓ completado[/green]"
        else:
            estado = "[yellow]⟳ en progreso[/yellow]"

        table.add_row(
            s["id"],
            f'"{s["frase"][:33]}{"…" if len(s["frase"]) > 33 else ""}"',
            str(total),
            f"[cyan]{bar}[/cyan] {done}/{total}",
            estado,
            s["created_str"],
        )

    console.print(table)
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")


def job_detail():
    console.print(Panel("[bold cyan]DETALLE DE JOB[/bold cyan]", border_style="cyan", padding=(0, 2)))
    console.print()

    if not _jobs:
        console.print("  [dim]No hay jobs enviados aún.[/dim]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    ids = [j["id"] for j in _jobs]
    console.print(f"  Jobs disponibles: [cyan]{', '.join(ids)}[/cyan]\n")
    job_id = Prompt.ask("  ID del job")

    s = _mock_job_status(job_id)
    if not s:
        console.print(f"  [red]✗ Job '{job_id}' no encontrado.[/red]\n")
        Prompt.ask("  [dim]Enter para continuar[/dim]", default="")
        return

    console.print()
    table = Table(box=box.ROUNDED, show_header=False, border_style="dim", padding=(0, 2))
    table.add_column(style="dim", width=16)
    table.add_column(style="white")

    done = s["done_workers"]
    total = s["workers"]
    bar = "█" * done + "░" * (total - done)

    estado_str = "[green]✓ completado[/green]" if s["status"] == "completado" else "[yellow]⟳ en progreso[/yellow]"

    table.add_row("job id", s["id"])
    table.add_row("frase original", f'"{s["frase"]}"')
    table.add_row("workers", f"{total}")
    table.add_row("progreso", f"[cyan]{bar}[/cyan]  {done}/{total} workers")
    table.add_row("estado", estado_str)
    table.add_row("enviado a las", s["created_str"])
    if s["status"] == "completado":
        table.add_row("resultado", f'[bold white]"{s.get("result", "—")}[/bold white]"')

    console.print(table)
    console.print()
    Prompt.ask("  [dim]Enter para continuar[/dim]", default="")



def main():
    while True:
        print_header()
        print_menu()
        choice = Prompt.ask("  [bold cyan]>[/bold cyan]", choices=["0", "1", "2", "3"], show_choices=False)

        if choice == "1":
            print_header()
            send_phrase()
        elif choice == "2":
            print_header()
            list_jobs()
        elif choice == "3":
            print_header()
            job_detail()
        elif choice == "0":
            console.print("\n  [dim]Hasta luego.[/dim]\n")
            break


if __name__ == "__main__":
    main()