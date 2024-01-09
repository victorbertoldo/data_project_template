# utils/cli.py
from rich.console import Console
from rich.progress import Progress
import time
from rich import print

console = Console()

def highlight_text(message, highlight, style):
    return message.replace(highlight, f"[{style}]{highlight}[/{style}]")

def print_success(message, highlight=None):
    if highlight:
        message = highlight_text(message, highlight, "bold green4")
    console.print(message)

def print_failure(message, highlight=None):
    if highlight:
        message = highlight_text(message, highlight, "bold red")
    console.print(message)

def print_warning(message, highlight=None):
    if highlight:
        message = highlight_text(message + ":warning:", highlight, "bold dark_red")
    console.print(message)

def print_info(message, highlight=None):
    if highlight:
        message = highlight_text(message, highlight, "bold yellow")
    console.print(message)

def progress_bar(task_description, total_steps):
    with Progress() as progress:
        task = progress.add_task(task_description, total=total_steps)
        while not progress.finished:
            progress.update(task, advance=1)
            time.sleep(0.1)  # Simulate work being done
