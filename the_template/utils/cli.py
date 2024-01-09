# utils/cli.py
from rich.console import Console
from rich.progress import Progress
import time
from rich import print

console = Console()

def highlight_text(message, highlight, style, emoji):
    # Add the emoji to the highlighted text
    highlighted_text = f"[{style}]{highlight} {emoji}[/{style}]"
    return message.replace(highlight, highlighted_text)

def print_success(message, highlight=None):
    emoji = ":heavy_check_mark:"
    style = "bold green4"
    if highlight:
        message = highlight_text(message, highlight, style, emoji)
    console.print(message)

def print_failure(message, highlight=None):
    emoji = ":cross:"
    style = "bold red"
    if highlight:
        message = highlight_text(message, highlight, style, emoji)
    console.print(message)

def print_warning(message, highlight=None):
    emoji = ":warning:"
    style = "bold dark_red"
    if highlight:
        message = highlight_text(message, highlight, style, emoji)
    console.print(message)

def print_info(message, highlight=None):
    emoji = " :bulb: "
    style = "bold yellow"
    if highlight:
        message = highlight_text(message, highlight, style, emoji)
    console.print(message)

def print_highlight(message, highlight=None):
    emoji = " :hammer: "
    style = "bold royal_blue1"
    if highlight:
        message = highlight_text(message, highlight, style, emoji)
    console.print(message)

def progress_bar(task_description, total_steps):
    with Progress() as progress:
        task = progress.add_task(task_description, total=total_steps)
        while not progress.finished:
            progress.update(task, advance=1)
            time.sleep(0.1)  # Simulate work being done
