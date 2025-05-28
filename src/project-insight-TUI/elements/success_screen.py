from textual.app import ComposeResult
from textual.containers import HorizontalGroup
from textual.screen import Screen
from textual.widgets import Label, Footer, Header, Button
from elements.menu_screen import MenuScreen

class SuccessScreen(Screen):
    CSS_PATH = "success_screen.tcss"  # Path to the CSS file for styling

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Success!", id="success_message")
        yield HorizontalGroup(
            Button(label="Main Menu", id="main_menu_button"),
            Button(label="Add Another User", id="add_another_button"),
            Button(label="Exit Application", id="exit_button"),
            id="action_buttons"
        )
        yield Footer()
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add_another_button":
            self.app.push_screen("add_user")
        if event.button.id == "exit_button":
            self.app.exit()
        if event.button.id == "main_menu_button":
            self.app.push_screen(MenuScreen())