from __future__ import annotations

import subprocess
from typing import List, Optional, Callable

from yts import yts_search, yts_movie_details, _render_movie_detail, YTSMovie, magnet_from_torrent


def run_yts_ui(key: str, timeout: float, retries: int, slow_after: float, verbose: bool) -> None:
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.widgets import DataTable, Footer, Header, Static, LoadingIndicator
        from textual.worker import Worker
    except Exception as exc:  # pragma: no cover - dependency guard
        raise SystemExit(
            "textual is required for --ui. Install with `pip install textual>=0.60`"
        ) from exc

    movies = yts_search(key, None, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
    if not movies:
        print("No results")
        return

    class _YTSBrowser(App):
        CSS_PATH = None
        BINDINGS = [
            Binding("q", "quit", "Quit"),
            Binding("b", "back", "Back to results"),
            Binding("enter", "show_detail", "Show detail"),
            Binding("c", "copy_cell", "Copy highlighted cell"),
        ]

        def __init__(self, movies: List[YTSMovie], fetch_detail: Callable[[str], Optional[str]]) -> None:
            super().__init__()
            self.movies = movies
            self.fetch_detail = fetch_detail
            self.detail_panel: Optional[Static] = None
            self.detail_summary: Optional[DataTable] = None
            self.detail_torrents: Optional[DataTable] = None
            self.table: Optional[DataTable] = None
            self._row_keys: List[str] = []
            self.spinner: Optional[LoadingIndicator] = None
            self.in_detail: bool = False

        def compose(self) -> ComposeResult:
            yield Header(show_clock=False)
            yield Static(
                "Tab/Arrow keys to move, Enter for details, b to go back, q to quit",
                id="help",
            )
            self.table = DataTable(zebra_stripes=True)
            self.table.add_columns("Title", "Year", "Rating", "YTS ID", "IMDb", "URL")
            for m in self.movies:
                self.table.add_row(
                    m.title,
                    str(m.year or ""),
                    f"{m.rating:.1f}" if m.rating else "",
                    str(m.id),
                    m.imdb_code,
                    m.url,
                    key=str(m.id),
                )
                self._row_keys.append(str(m.id))
            yield self.table
            self.spinner = LoadingIndicator(id="spinner")
            self.spinner.display = False
            yield self.spinner

            # Detail tables (hidden until a row is selected)
            self.detail_summary = DataTable(zebra_stripes=True)
            self.detail_summary.display = False
            self.detail_summary.visible = False
            self.detail_summary.add_columns("Title", "Year", "Rating", "Runtime", "YTS ID", "IMDb", "URL")
            yield self.detail_summary

            self.detail_torrents = DataTable(zebra_stripes=True)
            self.detail_torrents.display = False
            self.detail_torrents.visible = False
            self.detail_torrents.add_columns("Quality", "Type", "Size", "Seeds", "Peers", "Magnet")
            yield self.detail_torrents

            self.detail_panel = Static(
                "Select a movie and press Enter to view details", id="detail"
            )
            self.detail_panel.can_focus = True
            yield self.detail_panel
            yield Footer()

        def on_mount(self) -> None:
            if self.table:
                self.set_focus(self.table)

        def action_back(self) -> None:
            # If spinner is up, cancel detail and return to list
            if self.spinner and self.spinner.display:
                if self.spinner:
                    self.spinner.display = False
                if self.table:
                    self.table.display = True
                    self.table.visible = True
                    self.set_focus(self.table)
                return

            # Use explicit flag so focus changes across tables don't trigger exit
            if not self.in_detail:
                self.exit()
                return
            if self.table:
                self.table.display = True
                self.table.visible = True
                self.set_focus(self.table)
            if self.detail_panel:
                self.detail_panel.update("Select a movie and press Enter to view details")
                self.detail_panel.display = True
            if self.detail_summary:
                self.detail_summary.display = False
                self.detail_summary.visible = False
                self.detail_summary.clear()
            if self.detail_torrents:
                self.detail_torrents.display = False
                self.detail_torrents.visible = False
                self.detail_torrents.clear()
            if self.spinner:
                self.spinner.display = False
            self.in_detail = False

        def _current_row_key(self) -> Optional[str]:
            if not self.table:
                return None
            row_index = getattr(self.table, "cursor_row", None)
            if row_index is None:
                coord = getattr(self.table, "cursor_coordinate", None)
                row_index = getattr(coord, "row", None) if coord else None
            if row_index is None:
                return None
            try:
                return str(self.table.get_row_at(row_index).key)  # type: ignore[arg-type]
            except Exception:
                try:
                    return self._row_keys[int(row_index)]
                except Exception:
                    return None

        def action_show_detail(self) -> None:
            row_key = self._current_row_key()
            if not row_key:
                return
            self._start_spinner()
            self.run_worker(self._load_detail(row_key), thread=True)

        async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:  # type: ignore
            row_key = str(event.row_key)
            self._start_spinner()
            self.run_worker(self._load_detail(row_key), thread=True)

        def _start_spinner(self) -> None:
            if self.spinner:
                self.spinner.display = True
            if self.detail_panel:
                self.detail_panel.update("")
            if self.detail_summary:
                self.detail_summary.display = False
                self.detail_summary.visible = False
                self.detail_summary.clear()
            if self.detail_torrents:
                self.detail_torrents.display = False
                self.detail_torrents.visible = False
                self.detail_torrents.clear()

        def _stop_spinner(self) -> None:
            if self.spinner:
                self.spinner.display = False

        def _copy_to_clipboard(self, text: str) -> bool:
            if not text:
                return False
            # Prefer Textual clipboard API if available
            try:
                set_clipboard = getattr(self.app, "set_clipboard", None)
                if set_clipboard:
                    set_clipboard(str(text))
                    return True
            except Exception:
                pass
            # Fallback to pyperclip if installed
            try:
                import pyperclip

                pyperclip.copy(str(text))
                return True
            except Exception:
                pass
            # Fallback to platform clipboard tools
            for cmd in (["pbcopy"], ["wl-copy"], ["xclip", "-selection", "clipboard"]):
                try:
                    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
                    proc.communicate(str(text).encode("utf-8"), timeout=2)
                    if proc.returncode == 0:
                        return True
                except Exception:
                    continue
            return False

        def on_key(self, event) -> None:  # type: ignore
            key = getattr(event, "key", "")
            if key == "enter":
                event.stop()
                self.action_show_detail()

        async def _load_detail(self, row_key: str) -> None:
            movie = self.fetch_detail(row_key)
            self._stop_spinner()
            if not movie:
                if self.detail_panel:
                    self.detail_panel.update("No detail available")
                    self.detail_panel.display = True
                return
            self._render_detail(movie)

        def _render_detail(self, movie: dict) -> None:
            if self.table:
                self.table.display = False
                self.table.visible = False
            if self.detail_summary:
                self.detail_summary.display = True
                self.detail_summary.visible = True
                self.detail_summary.clear()
                self.detail_summary.add_row(
                    movie.get("title", ""),
                    str(movie.get("year") or ""),
                    f"{float(movie.get('rating') or 0):.1f}" if movie.get("rating") else "",
                    f"{movie.get('runtime')} min" if movie.get("runtime") else "",
                    str(movie.get("id") or ""),
                    movie.get("imdb_code") or "",
                    movie.get("url") or "",
                    key=str(movie.get("id") or ""),
                )
            if self.detail_torrents:
                self.detail_torrents.display = True
                self.detail_torrents.visible = True
                self.detail_torrents.clear()
                for t in movie.get("torrents") or []:
                    try:
                        mag = magnet_from_torrent(movie.get("title", ""), t)
                    except Exception:
                        mag = ""
                    self.detail_torrents.add_row(
                        t.get("quality") or "",
                        t.get("type") or "",
                        t.get("size") or "",
                        str(t.get("seeds") or ""),
                        str(t.get("peers") or ""),
                        mag,
                    )
            if self.detail_panel:
                self.detail_panel.display = True
                self.detail_panel.update("Press b to go back, q to quit")
            if self.detail_summary:
                self.set_focus(self.detail_summary)
                try:
                    self.detail_summary.focus()
                except Exception:
                    pass
            self.in_detail = True

        def action_copy_cell(self) -> None:
            target = self.focused
            if not target:
                return
            if not isinstance(target, DataTable):
                return
            coord = getattr(target, "cursor_coordinate", None)
            row = getattr(coord, "row", None) if coord else getattr(target, "cursor_row", None)
            col = getattr(coord, "column", None) if coord else getattr(target, "cursor_column", None)
            if row is None or col is None:
                return
            try:
                value = target.get_cell_at((row, col))
            except Exception:
                try:
                    value = target.get_row_at(row)[col]
                except Exception:
                    value = ""
            text = "" if value is None else str(value)
            copied = self._copy_to_clipboard(text)
            if self.detail_panel:
                if copied:
                    preview = text if len(text) <= 80 else text[:77] + "..."
                    self.detail_panel.update(f"Copied: {preview}")
                else:
                    self.detail_panel.update("Copy failed (no clipboard tool available)")

    def fetch_detail(row_key: str) -> Optional[dict]:
        movie = yts_movie_details(row_key, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
        if not movie:
            movie = yts_movie_details(
                "tt" + row_key if not row_key.startswith("tt") else row_key,
                timeout=timeout,
                retries=retries,
                slow_after=slow_after,
                verbose=verbose,
            )
        return movie

    app = _YTSBrowser(movies, fetch_detail=fetch_detail)
    app.run()
