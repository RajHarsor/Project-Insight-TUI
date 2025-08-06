from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup
from textual.widgets import Label, Button, Footer, Header
from ..elements.menu_screen import MenuScreen
from ..elements.check_individual_compliance_screen import CheckIndividualComplianceScreen

class GenerateReportScreen(Screen):
    CSS_PATH = "generate_report_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Please select if you want to check individual compliance or generate a report for the last 24 hours", id = "report_message")
        yield HorizontalGroup(
            Button(label="Generate Report", id="generate_report_button"),
            Button(label="Check Individual Compliance", id="check_individual_compliance_button"),
            Button(label="Back to Main Menu", id="back_to_main_menu_button"),
            Button(label="Exit Application", id="exit_button"),
            id="action_buttons"
        )
        yield Footer()
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "generate_report_button":
            self.app.push_screen("generate_report") #TODO Make this screen
        elif event.button.id == "check_individual_compliance_button":
            self.app.push_screen(CheckIndividualComplianceScreen())
        elif event.button.id == "exit_button":
            self.app.exit()
        elif event.button.id == "main_menu_button":
            self.app.push_screen(MenuScreen())