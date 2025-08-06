from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup
from textual.widgets import Label, Button, Footer, Header, Input, DataTable
from ..elements.menu_screen import MenuScreen
from ..methods.dynamoDB_methods import get_item_from_dynamodb
import datetime

# First column should be date_range that we calculated


class CheckIndividualComplianceScreen(Screen):
    CSS_PATH = "check_individual_compliance_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Please select a user to check compliance:", id="compliance_message")
        yield Input(placeholder="Enter participant ID", id="user_input")
        yield Label("", id="start_end_date")
        yield Label("", id="compliance_result")
        # Create Datatable with 14 columns and 5 rows
        yield DataTable(id="compliance_data_table")
        
        yield HorizontalGroup(
            Button(label="Check Compliance", id="check_compliance_button"),
            Button(label="Back to Main Menu", id="main_menu_button"),
            id="action_buttons"
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "check_compliance_button":
            # Query the input field for the participant ID
            participant_id = self.query_one("#user_input", Input).value
            user_data = get_item_from_dynamodb(participant_id)
            if user_data:
                start_date = user_data.get("study_start_date", "N/A")
                end_date = user_data.get("study_end_date", "N/A")
                self.query_one("#start_end_date", Label).update(f"Study Start Date: {start_date} | Study End Date: {end_date}")
                
                # Convert start_date and end_date into dateTime objects
                try:
                    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    self.query_one("#compliance_result", Label).update("Invalid date format in user data.")
                    return
                # Make a list of dates between start_date and end_date (inclusive)
                date_range = [start_date_obj + datetime.timedelta(days=i) for i in range((
                    end_date_obj - start_date_obj).days + 1)]
                date_strings = [d.strftime("%Y-%m-%d") for d in date_range]

                
                data_table = self.query_one("#compliance_data_table", DataTable)
                data_table.clear()
                data_table.add_columns(*date_strings)  # Set columns as dates
                for _ in range(4):
                    data_table.add_row(*[""] * len(date_strings))
                
                # Show compliance data table since it's hidden by default
                data_table.styles.display = "block"  # Show the table
                
                
        elif event.button.id == "main_menu_button":
            self.app.push_screen(MenuScreen())