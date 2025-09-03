from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup
from textual.widgets import Label, Button, Footer, Header
from ..elements.menu_screen import MenuScreen

class ReportGenerationScreen(Screen):
    CSS_PATH = "report_generation_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("1. Choose a date and save location \n 2. Generate the report!", id = "report_generation_message")
        yield HorizontalGroup(
            Button(label="Back to Main Menu", id="back_to_main_menu_button"),
            Button(label="Exit Application", id="exit_button"),
            id="action_buttons"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "exit_button":
            self.app.exit()
        elif event.button.id == "back_to_main_menu_button":
            self.app.push_screen(MenuScreen())