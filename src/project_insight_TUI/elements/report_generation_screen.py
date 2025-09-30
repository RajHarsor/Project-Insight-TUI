from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup, VerticalGroup
from textual.widgets import Label, Button, Footer, Header, Input
from textual import work
from ..elements.menu_screen import MenuScreen
import sys
import subprocess
from ..methods.compliance_methods import generate_compliance_report

# Platform-specific imports
if sys.platform != "darwin":
    from tkinter import Tk, filedialog


class ReportGenerationScreen(Screen):
    CSS_PATH = "report_generation_screen.tcss"

    def __init__(self) -> None:
        super().__init__()
        self.file_path = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Report Generation Page", id="title_label")
        yield VerticalGroup(
            Label("1. Choose a date and save location", id = "instruction_label1"),
            Label("2. Generate the report!", id = "instruction_label2"),
            Label("Note: If the report looks incorrect, please open Google Drive and close and re-open the application.", id="instruction_label3"),
            id="instructions_container"
        )
        yield Input(placeholder="Enter Date (YYYY-MM-DD)", id="date_input")
        yield Button("Choose Save Location", id="save_location_button")
        yield Label("", id="status_label")
        yield HorizontalGroup(
            Button(label="Back to Main Menu", id="back_to_main_menu_button"),
            Button(label="Generate Report", id="generate_report_button"),
            Button(label="Exit Application", id="exit_button"),
            id="action_buttons"
        )
        yield Footer()
    
    @work(exclusive=True, thread=True)
    def select_save_location(self) -> None:
        """Opens a file dialog in a background thread using a non-blocking method."""
        path = ""
        try:
            if sys.platform == "darwin":
                # macOS: Use osascript
                script = 'POSIX path of (choose folder with prompt "Select Report Save Location")'
                result = subprocess.run(
                    ['osascript', '-e', script],
                    capture_output=True, text=True, check=True
                )
                path = result.stdout.strip()
            elif sys.platform == "win32":
                # Windows: Use PowerShell
                script = """
                Add-Type -AssemblyName System.windows.forms
                $folderBrowser = New-Object System.Windows.Forms.FolderBrowserDialog
                $folderBrowser.Description = "Select Report Save Location"
                if ($folderBrowser.ShowDialog() -eq "OK") {
                    return $folderBrowser.SelectedPath
                }
                """
                result = subprocess.run(
                    ["powershell", "-Command", script],
                    capture_output=True, text=True, check=True
                )
                path = result.stdout.strip()
            else:
                # Linux/Other: Fallback to tkinter (might still have issues depending on environment)
                # This part remains as it was, as it's the best fallback without adding
                # dependencies like zenity or kdialog.
                root = Tk()
                root.withdraw()
                path = filedialog.askdirectory(title="Select Report Save Location")
                root.destroy()
        except (subprocess.CalledProcessError, FileNotFoundError):
            # User cancelled or an error occurred
            path = ""
        
        if path:
            self.app.call_from_thread(self.update_path_and_label, path)

    def update_path_and_label(self, path: str) -> None:
        """Callback to update the file path and status label from the main thread."""
        self.file_path = path
        self.query_one("#status_label", Label).update(f"Save location set to: {path}")

    @work(exclusive=True, thread=True)
    def run_report_generation(self, date: str, path: str) -> None:
        """Runs the report generation in a background thread."""
        status_label = self.query_one("#status_label", Label)
        try:
            generate_compliance_report(date, path)
            # Use call_from_thread to update UI from worker
            self.app.call_from_thread(
                status_label.update, f"Report generated and saved to {path}/reports"
            )
        except Exception as e:
            self.app.call_from_thread(
                status_label.update, f"Error generating report: {e}"
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "exit_button":
            self.app.exit()
        elif event.button.id == "back_to_main_menu_button":
            self.app.push_screen(MenuScreen())
        elif event.button.id == "save_location_button":
            self.select_save_location()
        elif event.button.id == "generate_report_button":
            if self.file_path:
                date_input = self.query_one("#date_input", Input).value
                status_label = self.query_one("#status_label", Label)
                if date_input:
                    status_label.update("Generating report, please wait...")
                    # Run the report generation in a worker
                    self.run_report_generation(date_input, self.file_path)
                else:
                    status_label.update("Please enter a valid date.")
            else:
                self.query_one("#status_label", Label).update("Please select a save location first.")