# Project-Insight-TUI

TUI for Project INSIGHT.

## Notes
This application must be run on a normal computer and will not work using Citrix Workspace. This is due to the some weird location saving issues with the TUI and Citrix Workspace.

## Prerequisites
- $\ge$ Python 3.12 (https://www.python.org/downloads/; make sure to add Python to your PATH and install pip)
- git (https://git-scm.com/downloads; make sure to add git to your PATH)
- **Optional** - Google Drive for Desktop (https://www.google.com/intl/en_in/drive/download/)
  - Necessary if you need the ability to do report generation

## Installation

1. Open up the command prompt (Windows) or terminal (Linux/Mac).
2. Put the following command into the command prompt or terminal to download
   ```bash
   pip install git+https://github.com/RajHarsor/Project-Insight-TUI
   ```
3. After the installation is complete, you can run the application by typing
   ```bash
   insight
   ```
4. The TUI should now be running in your terminal!

## Updating and  Uninstalling
To update the application, you can run the following command:
```bash
pip install --upgrade git+https://github.com/RajHarsor/Project-Insight-TUI
```
To uninstall the application, you can run the following command:
```bash
pip uninstall project-insight-tui
```

## TODO
- [ ] Implement report generation.
- [ ]  Implement manual text message sending.
- [ ]  Implement CloudWatch logs viewing.
- [ ]  Implement Cloudwatch alarms (AWS Backend).

## License
This project is licensed under the MIT License.

## Issues and Contact
If you encounter any issues or have questions, feel free to open an issue on the [GitHub repository](https://github.com/RajHarsor/Project-Insight-TUI/issues) and/or contact harsora@rowan.edu.