# SQLite Editor (Streamlit)

A robust SQLite database editor built with Streamlit for macOS. Features native file selection (Finder), CRUD operations using `rowid`, and data import from Excel/CSV.

## 🚀 Features
- **Native File Selection**: Open any SQLite database using macOS Finder.
- **Robust Data Editor**: Interactive table editor with automatic tracking via `rowid`.
- **Data Import**: Import data from Excel (`.xlsx`) or CSV (`.csv`) with preview and Append/Replace modes.
- **SQL Executor**: Run raw SQL queries directly from the UI.

## 🛠️ How to Run
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the application:
   ```bash
   streamlit run main.py
   ```

## ⚙️ Technical Details
- Uses `osascript` (macOS) for native file dialogs to avoid threading issues.
- Leverages SQLite's `rowid` for reliable row identification.
- Persistent session state for a smoother user experience.
