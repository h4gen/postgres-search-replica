from typing import Any, Dict, List
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.syntax import Syntax
from rich.columns import Columns
from rich.console import Group

console = Console()

def render_status_box(pipeline_name: str, summary: Dict[str, Any]) -> Panel:
    """
    Renders the 'Glass Cockpit' status dashboard for a single pipeline.
    Spec:
    ╭── products ───────────────────────────────────────────────╮
    │ Status: 🟢 Ready   Lag: 0s   Mode: CDC (Streaming)        │
    ├───────────────────────────────────────────────────────────┤
    │ Source: postgres/app     Sink: qdrant/prod                │
    │ Schema: [id, name, desc] Model: openai/small              │
    ╰───────────────────────────────────────────────────────────╯
    """
    # 1. Extract Details
    # Defaults
    status = "🟢 Ready"
    lag = "0s"
    mode = "CDC (Streaming)"
    source = "postgres"
    sink = "postgres"
    schema = "[]"
    model = "n/a"
    
    # Try to parse summary (which is the result of /control-plane/summary)
    pipeline_summary = summary.get("pipeline", {})
    
    # Status Logic
    pending = 0
    vectorizers = pipeline_summary.get("vectorizers", [])
    for v in vectorizers:
        # Check if this vectorizer belongs to the pipeline (matches prefix or name)
        if pipeline_name in v.get("name", "") or pipeline_name in v.get("source_table", ""):
            pending += v.get("pending_items", 0) or 0
            
    if pending > 0:
        status = f"🟡 Syncing ({pending} pending)"
        
    # Lag Logic
    lag_val = pipeline_summary.get("lag_mb", 0)
    lag = f"{lag_val:.1f} MB" if lag_val > 0 else "0s"
    
    # Mode
    # Ideally we'd know if it's CDC or Bulk. For now, we can infer from config if present.
    
    # Config details (from summary["config_summaries"][pipeline_name])
    config_summary = summary.get("config_summaries", {}).get(pipeline_name, {})
    model = config_summary.get("model", model)
    # The summary includes version_id and search_profile
    
    # Projections (from summary["projections"][pipeline_name])
    proj = summary.get("projections", {}).get(pipeline_name, {})
    row_count = proj.get("row_count", "n/a")
    
    # Content Construction
    grid = Table.grid(expand=True)
    grid.add_column()
    grid.add_column(justify="right")
    
    # Row 1: Status Line
    grid.add_row(
        f"Status: {status}   Lag: {lag}", 
        f"Mode: {mode}"
    )
    
    details_grid = Table.grid(expand=True)
    details_grid.add_column()
    details_grid.add_column()
    
    details_grid.add_row(
        f"Source: [cyan]{source}[/cyan]", 
        f"Sink: [magenta]{sink}[/magenta]"
    )
    details_grid.add_row(
        f"Rows: [bold]{row_count}[/bold]",
        f"Model: [green]{model}[/green]"
    )

    content = Group(
        grid,
        Text("─" * 60, style="dim"),
        details_grid
    )

    return Panel(
        content,
        title=f"[bold]{pipeline_name}[/bold]",
        border_style="blue",
        expand=False
    )

def render_diff(pipeline_name: str, actions: List[str], projections: Dict[str, Any]) -> Panel:
    """
    Renders the Git-Style Diff for a plan.
    """
    # Actions (Diff)
    diff_text = Text()
    for action in actions:
        if "Create" in action or "Add" in action or "Setup" in action:
            diff_text.append(f"+ {action}\n", style="green")
        elif "Drop" in action or "Remove" in action:
            diff_text.append(f"- {action}\n", style="red")
        else:
            diff_text.append(f"  {action}\n", style="white")

    # Projections (Impact)
    impact_table = Table(show_header=False, box=None)
    impact_table.add_row("[bold]Est. Cost:[/bold]", f"${projections.get('estimated_cost_usd', 0)}")
    impact_table.add_row("[bold]Est. Time:[/bold]", "~12 mins (simulated)") # We need real estimation logic later
    impact_table.add_row("[bold]Storage:[/bold]", f"{projections.get('estimated_ram_mb', 0)} MB RAM")

    content = Group(
        diff_text,
        Text("─" * 60, style="dim"),
        impact_table
    )

    return Panel(
        content,
        title=f"📋 Plan for '[bold]{pipeline_name}[/bold]'",
        border_style="yellow",
        expand=False
    )

def render_comparison(results_a: List[Dict[str, Any]], results_b: List[Dict[str, Any]]) -> Table:
    """
    Renders Side-by-Side comparison table.
    """
    table = Table(title="Side-by-Side Verification", show_lines=True)
    table.add_column("Rank", justify="center", style="dim")
    table.add_column("v1 (Baseline)", ratio=1)
    table.add_column("Score", justify="right")
    table.add_column("v2 (Experiment)", ratio=1)
    table.add_column("Score", justify="right")
    table.add_column("Diff", justify="right")

    # Normalize lengths
    max_len = max(len(results_a), len(results_b))
    
    for i in range(max_len):
        row_a = results_a[i] if i < len(results_a) else None
        row_b = results_b[i] if i < len(results_b) else None
        
        content_a = row_a['content'][:50]+"..." if row_a else "-"
        score_a = row_a.get('score', row_a.get('distance', 0)) if row_a else 0
        
        content_b = row_b['content'][:50]+"..." if row_b else "-"
        score_b = row_b.get('score', row_b.get('distance', 0)) if row_b else 0
        
        diff = score_b - score_a
        diff_styled = f"[green]+{diff:.2f}[/green]" if diff > 0 else f"[red]{diff:.2f}[/red]"
        
        table.add_row(
            str(i+1),
            content_a,
            f"{score_a:.2f}",
            content_b,
            f"{score_b:.2f}",
            diff_styled
        )
        
    return table
