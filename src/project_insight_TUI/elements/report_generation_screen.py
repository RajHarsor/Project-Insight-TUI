from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup, VerticalGroup
from textual.widgets import Label, Button, Footer, Header, Input
from ..elements.menu_screen import MenuScreen
from tkinter import filedialog
class ReportGenerationScreen(Screen):
    CSS_PATH = "report_generation_screen.tcss"
    
    def __init__(self):
        super().__init__()
        self.save_location = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Report Generation Page", id="title_label")
        yield VerticalGroup(
            Label("1. Choose a date and save location", id = "instruction_label1"),
            Label("2. Generate the report!", id = "instruction_label2"),
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
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "exit_button":
            self.app.exit()
        elif event.button.id == "back_to_main_menu_button":
            self.app.push_screen(MenuScreen())
        elif event.button.id == "save_location_button":
            path = filedialog.askdirectory(title="Select Report Save Location")
            if path:
                self.save_location = path
                self.query_one("#status_label", Label).update(f"Save location set to: {self.save_location}")