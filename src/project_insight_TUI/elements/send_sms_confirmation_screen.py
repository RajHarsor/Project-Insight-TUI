from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.widgets import Button, Label
from textual.containers import HorizontalGroup
from ..elements.menu_screen import MenuScreen

class SendSMSConfirmationScreen(ModalScreen):
    def __init__(
        self,
        participant_id: str,
        custom_message: str,
        premade_button_text: str
    ):
        super().__init__()
        self.participant_id = participant_id
        self.custom_message = custom_message
        self.premade_button_text = premade_button_text

    # Path to the CSS file for styling
    CSS_PATH = "send_sms_confirmation_screen.tcss"

    def compose(self) -> ComposeResult:
        if self.custom_message is None:
            yield Label(f"Are you sure you want to send \"{self.premade_button_text}\" to {self.participant_id}?", id='confirmation_message')
        elif self.premade_button_text is None:
            yield Label(f"Are you sure you want to send \"{self.custom_message}\" to {self.participant_id}?", id='confirmation_message')
        else:
            yield Label("An error occured, please restart the application.", id='confirmation_message')
        yield Label("", id='status_message')
        yield HorizontalGroup(
            Button("Go to Main Menu", id="main_menu_button"),
            Button("Go Back", id="cancel_button"),
            Button("Confirm", id="confirm_button"),
            id="button_group"
        )
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "confirm_button":
            # TODO Implement the logic to send the SMS with data from sns.publish. Check for failures too when receiving response
            # Disable confirm button to prevent multiple submissions
            self.query_one("#confirm_button").disabled = True
            self.query_one("#cancel_button").disabled= True
            # Show status message
            self.query_one("#status_message").update("SMS sent successfully! If you want to send another SMS, you need to close the application and restart it.")
            # Display the status message
            self.query_one("#status_message").display = True
        elif button_id == "cancel_button":
            self.app.pop_screen()
        elif button_id == "main_menu_button":
            self.app.push_screen(MenuScreen())