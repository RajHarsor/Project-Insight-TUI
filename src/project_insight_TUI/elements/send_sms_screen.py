from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header, Button, Select, Input, Label, TextArea
from textual.containers import VerticalGroup, HorizontalGroup
from datetime import datetime, timezone
from ..methods.dynamoDB_methods import get_item_from_dynamodb
from textual import on
from ..elements.send_sms_confirmation_screen import SendSMSConfirmationScreen

LINES="""Custom Message
EMA Survey 1A (link with leaderboard)
EMA Survey 1B (link without leaderboard)
EMA Survey 2
EMA Survey 3
EMA Survey 4""".splitlines()


class SendSMSScreen(Screen):
    CSS_PATH = "send_sms_screen.tcss"  
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Select a Participant ID to send SMS:", id="send_sms_label")
        yield HorizontalGroup(
            Input(placeholder="Participant ID", id="participant_id_input", type='integer'),
            Button("Search for Participant", id="search_participant_button"),
            id="participant_search_group"
        )
        yield Label("", id = 'participant_search_info', disabled=True)
        yield Label("Select the type of message you would like to send:", id="message_type_label")
        yield HorizontalGroup(
            Select(((line, line) for line in LINES), id="message_type_select", prompt="Select a message type", disabled=True),
            Label("", id='premade_button_text', disabled=True),
            TextArea(id="custom_message_input", disabled=True),
            id="message_input_group"
        )
        yield HorizontalGroup(
            Button("Go Back", id="back_to_menu_button"),
            Button("Send SMS", id="send_sms_button", disabled=True),
            id="action_buttons"
        )

    def on_button_pressed(self, event) -> None:
        button_id = event.button.id
        
        if button_id == "search_participant_button":
            participant_id = self.query_one("#participant_id_input", Input).value
            user_details_label = self.query_one("#participant_search_info", Label)
            message_type_select = self.query_one("#message_type_select", Select)
            
            # Check if participant_id is empty
            if not participant_id or participant_id.strip() == "":
                user_details_label.update("Please enter a Participant ID.")
                user_details_label.disabled = False
                return
            
            try:
                user_data = get_item_from_dynamodb(participant_id)
                self.user_data = user_data  # Store user_data as an instance variable
                
                if user_data is None:
                    user_details_label.update("User Not Found.")
                    user_details_label.disabled = False
                    return
                
                if user_data == {}:
                    user_details_label.update("User Not Found (empty response).")
                    user_details_label.disabled = False
                    return
                
                # Get study start date and calculate day of study
                study_start_date = user_data.get('study_start_date')
                if study_start_date:
                    start_date = datetime.strptime(study_start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    participant_day = (datetime.now(timezone.utc) - start_date).days + 1

                # Show user data
                details = f"Participant ID: {user_data['participant_id']}\n" \
                          f"Study Start Date: {user_data.get('study_start_date', 'N/A')}\n" \
                          f"Study End Date: {user_data.get('study_end_date', 'N/A')}\n" \
                          f"Phone Number: {user_data.get('phone_number', 'N/A')}\n" \
                          f"Schedule Type: {user_data.get('schedule_type', 'N/A')}\n" \
                          f"Leaderboard Link: {user_data.get('lb_link', 'N/A')}\n" \
                          f"Day in Study: {participant_day}"
                user_details_label.update(details)
                user_details_label.disabled = False
                message_type_label = self.query_one("#message_type_label", Label)
                message_type_label.display = True
                message_type_select = self.query_one("#message_type_select", Select)
                message_type_select.display = True
                message_type_label.disabled = False
                message_type_select.disabled = False
                
            except Exception as e:
                user_details_label.update(f"Error retrieving user data: {str(e)}")
                user_details_label.disabled = False
                return
            
        elif button_id == "back_to_menu_button":
            self.app.pop_screen()
        
        elif button_id == "send_sms_button":
            # Send Participant # and the full message to the SendSMSConfirmationScreen
            participant_id = self.query_one("#participant_id_input", Input).value
            custom_message_input = self.query_one("#custom_message_input", TextArea).text
            if custom_message_input.strip() == "":
                custom_message_input = None
            premade_button_text = str(self.query_one("#premade_button_text", Label).renderable)
            if premade_button_text.strip() == "":
                premade_button_text = None
            self.app.push_screen(SendSMSConfirmationScreen(participant_id=participant_id, custom_message=custom_message_input, premade_button_text=premade_button_text))

    @on(Select.Changed)
    def on_message_type_changed(self, event: Select.Changed) -> None:
        selected_option = event.select.value
        if selected_option == "Custom Message":
            # Enable the custom message input field and disable the premade button text
            self.query_one("#custom_message_input", TextArea).disabled = False
            self.query_one("#premade_button_text", Label).disabled = True
            premade_button_text = self.query_one("#premade_button_text", Label)
            premade_button_text.display = False
            # Display the custom message input field - normally its hidden
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = True
            # Enable the send SMS button
            self.query_one("#send_sms_button", Button).disabled = False
        elif selected_option == "EMA Survey 1A (link with leaderboard)":
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = False
            link = getattr(self, "user_data", {}).get('lb_link', '')
            if link:
                message = f"Hello from the Project INSIGHT Team at Rowan University. At your earliest convenience please take this survey: {link}. If you have any questions please reach out to us at projectinsight@rowan.edu. Thank you!"
                self.query_one("#premade_button_text", Label).update(message)
                self.query_one("#premade_button_text", Label).disabled = False
                self.query_one("#custom_message_input", TextArea).disabled = True
                self.query_one("#send_sms_button", Button).disabled = False
            else:
                self.query_one("#premade_button_text", Label).update("No leaderboard link available.")
                self.query_one("#premade_button_text", Label).disabled = False
                self.query_one("#custom_message_input", TextArea).disabled = True
                self.query_one("#send_sms_button", Button).disabled = True
                self.query_one("#send_sms_button", Button).disabled = True
        elif selected_option == "EMA Survey 1B (link without leaderboard)":
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = False
            link = "https://rowan.co1.qualtrics.com/jfe/form/SV_869PIgLB4XwPD5s"
            message = f"Hello from the Project INSIGHT Team at Rowan University. At your earliest convenience please take this survey: {link}. If you have any questions please reach out to us at projectinsight@rowan.edu. Thank you!"
            self.query_one("#premade_button_text", Label).update(message)
            premade_button_text = self.query_one("#premade_button_text", Label)
            premade_button_text.display = True
            self.query_one("#premade_button_text", Label).disabled = False
            self.query_one("#custom_message_input", TextArea).disabled = True
            self.query_one("#send_sms_button", Button).disabled = False
        elif selected_option == "EMA Survey 2":
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = False
            link = "https://rowan.co1.qualtrics.com/jfe/form/SV_aWBwIQVfSMS3kk6"
            message = f"Hello from the Project INSIGHT Team at Rowan University. At your earliest convenience please take this survey: {link}. If you have any questions please reach out to us at projectinsight@rowan.edu. Thank you!"
            self.query_one("#premade_button_text", Label).update(message)
            premade_button_text = self.query_one("#premade_button_text", Label)
            premade_button_text.display = True
            self.query_one("#premade_button_text", Label).disabled = False
            self.query_one("#custom_message_input", TextArea).disabled = True
            self.query_one("#send_sms_button", Button).disabled = False
        elif selected_option == "EMA Survey 3":
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = False
            link = "https://rowan.co1.qualtrics.com/jfe/form/SV_efH33MAvT9pkTr0"
            message = f"Hello from the Project INSIGHT Team at Rowan University. At your earliest convenience please take this survey: {link}. If you have any questions please reach out to us at projectinsight@rowan.edu. Thank you!"
            self.query_one("#premade_button_text", Label).update(message)
            premade_button_text = self.query_one("#premade_button_text", Label)
            premade_button_text.display = True
            self.query_one("#premade_button_text", Label).disabled = False
            self.query_one("#custom_message_input", TextArea).disabled = True
            self.query_one("#send_sms_button", Button).disabled = False
        elif selected_option == "EMA Survey 4":
            custom_message_input = self.query_one("#custom_message_input", TextArea)
            custom_message_input.display = False
            link = "https://rowan.co1.qualtrics.com/jfe/form/SV_bC8e4N2Nqu3234i"
            message = f"Hello from the Project INSIGHT Team at Rowan University. At your earliest convenience please take this survey: {link}. If you have any questions please reach out to us at projectinsight@rowan.edu. Thank you!"
            self.query_one("#premade_button_text", Label).update(message)
            premade_button_text = self.query_one("#premade_button_text", Label)
            premade_button_text.display = True
            self.query_one("#premade_button_text", Label).disabled = False
            self.query_one("#custom_message_input", TextArea).disabled = True
            self.query_one("#send_sms_button", Button).disabled = False