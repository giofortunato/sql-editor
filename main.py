import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import os
import tempfile
import sys

# Environment Detection
IS_CLOUD = os.getenv("STREAMLIT_RUNTIME_ENV_CLOUD") == "true" or sys.platform not in ["darwin", "win32"]

# Page config
st.set_page_config(
    page_title="SQLite Editor",
    page_icon="💾",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .stAppHeader {
        background-color: #f0f2f6;
    }
    .main .block-container {
        padding-top: 2rem;
    }
    h1 {
        color: #0e1117;
    }
    h2, h3 {
        color: #262730;
    }
    .stButton button {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

import subprocess

# Helper for database selection
def select_file():
    try:
        # macOS: AppleScript native dialog
        cmd = """osascript -e 'set theFile to choose file with prompt "Select SQLite Database" of type {"db", "sqlite", "sqlite3"}
        POSIX path of theFile'"""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except Exception as e:
        st.error(f"Error opening file dialog: {e}")
        return None

# Sidebar - Database Selection
with st.sidebar:
    st.title("🗄️ Database Navigator")
    
    if IS_CLOUD:
        st.subheader("Upload Database")
        uploaded_db = st.file_uploader("Upload a SQLite .db file", type=["db", "sqlite", "sqlite3"], key="db_uploader")
        
        if uploaded_db:
            # Handle new file upload: only write to disk if it's new or missing
            if "last_uploaded_file" not in st.session_state or st.session_state["last_uploaded_file"] != uploaded_db.name:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                    tmp_file.write(uploaded_db.getbuffer())
                    st.session_state["db_path"] = tmp_file.name
                    st.session_state["last_uploaded_file"] = uploaded_db.name
                st.rerun() # Refresh to ensure engine is initialized with new path

            db_url = f"sqlite:///{st.session_state['db_path']}"
            st.success(f"Loaded: `{uploaded_db.name}`")
            
            # Persistent download button for the current modified version
            try:
                with open(st.session_state["db_path"], "rb") as f:
                    btn = st.download_button(
                        label="📥 Download Modified DB",
                        data=f,
                        file_name=uploaded_db.name,
                        mime="application/x-sqlite3",
                        help="Scarica il database con tutte le modifiche apportate finora."
                    )
            except Exception as e:
                st.error(f"Error preparing download: {e}")
            
            st.info("⚠️ Le modifiche sono temporanee. Scarica il file per salvarle permanentemente.")
        else:
            # Clear state if file is removed
            if "db_path" in st.session_state:
                if st.session_state["db_path"] and os.path.exists(st.session_state["db_path"]):
                    try: os.remove(st.session_state["db_path"])
                    except: pass
                st.session_state["db_path"] = None
                st.session_state["last_uploaded_file"] = None
            
            st.info("Upload a database to start.")
            st.stop()
    else:
        st.subheader("Select Database")
        
        # Initialize session state for db_path
        if "db_path" not in st.session_state:
            st.session_state["db_path"] = None

        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("📂 Open File"):
                selected_file = select_file()
                if selected_file:
                    st.session_state["db_path"] = selected_file
                    st.rerun()
        
        selected_db_path = st.session_state["db_path"]

        if selected_db_path:
            if os.path.exists(selected_db_path):
                st.success(f"Loaded: `{os.path.basename(selected_db_path)}`")
                st.caption(f"Path: {selected_db_path}")
                db_url = f"sqlite:///{selected_db_path}"
            else:
                st.error("Selected file not found. Please select again.")
                st.session_state["db_path"] = None
                st.stop()
        else:
            st.info("Please select a database file.")
            st.stop()

# Database Connection
@st.cache_resource(ttl="1h") # clear cache if args change or after time
def get_engine(db_url):
    return create_engine(db_url)

try:
    engine = get_engine(db_url)
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# Helper functions
def get_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()

def load_data(table_name, engine):
    with engine.connect() as conn:
        try:
            # Fetch rowid as a standard column
            query = text(f"SELECT rowid, * FROM {table_name}")
            df = pd.read_sql(query, conn)
            # Do NOT set index to rowid, keep it as a column
            return df
        except Exception as e:
            # Fallback
            return pd.read_sql_table(table_name, conn)

def save_changes(table_name, original_df, edited_df, engine):
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # We assume 'rowid' column exists. 
            # If fallback load_data was used, we might rely on standard index (not handled here for simplicity, assuming rowid works)
            
            if 'rowid' not in original_df.columns:
                 st.error("Cannot save changes: 'rowid' column missing.")
                 return

            # Convert rowid to numeric, coercing errors (new rows might have empty strings or None)
            # st.data_editor might return None or NaN for new rows' rowid
            
            # Helper to get valid rowids
            def get_ids(df):
                return set(df['rowid'].dropna().astype(int))

            orig_ids = get_ids(original_df)
            
            # For edited_df, we need to handle potential new rows where rowid is None/NaN
            # We treat rows with valid rowid as "existing", others as "new"
            
            # 1. Handle Deletions
            # IDs in original but not in edited (looking at the 'rowid' column)
            # Note: edited_df might have NaN for rowids of new rows.
            current_ids = get_ids(edited_df)
            deleted_ids = orig_ids - current_ids
            
            if deleted_ids:
                # Ensure all are integers and create a comma-separated string
                # This avoids SQLAlchemy bind parameter issues with 'IN' clauses in text()
                ids_str = ",".join(str(int(x)) for x in deleted_ids)
                delete_query = text(f"DELETE FROM {table_name} WHERE rowid IN ({ids_str})")
                conn.execute(delete_query)
            
            # 2. Handle Additions
            # Rows in edited_df where rowid is NaN/None
            # We check if rowid is null or not in orig_ids (though mostly null for new rows)
            new_rows = edited_df[edited_df['rowid'].isna()]
            
            if not new_rows.empty:
                # Prepare for insertion: drop 'rowid' column as SQLite assigns it
                rows_to_insert = new_rows.drop(columns=['rowid'])
                rows_to_insert.to_sql(table_name, conn, if_exists='append', index=False)
                
            # 3. Handle Updates
            # IDs in both. Compare content.
            common_ids = orig_ids & current_ids
            existing_rows_edited = edited_df[edited_df['rowid'].isin(common_ids)].set_index('rowid')
            existing_rows_orig = original_df[original_df['rowid'].isin(common_ids)].set_index('rowid')
            
            changes_count = 0
            for rid in common_ids:
                row_new = existing_rows_edited.loc[rid]
                row_orig = existing_rows_orig.loc[rid]
                
                if not row_new.equals(row_orig):
                    # Create update dict with sanitized keys for binding
                    # SQLAlchemy text() bind params cannot contain spaces
                    
                    raw_update_data = row_new.to_dict()
                    clean_update_data = {}
                    set_clauses = []
                    
                    for col, val in raw_update_data.items():
                        # Create a safe bind key (e.g. "Descrizione Spoke" -> "Descrizione_Spoke")
                        clean_key = col.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "")
                        
                        # Handle duplicate keys if any (unlikely but good practice)
                        if clean_key in clean_update_data:
                            clean_key = f"{clean_key}_{hash(col)}"
                            
                        clean_update_data[clean_key] = val
                        # Use quoted identifiers for column names to handle spaces/special chars in DB
                        set_clauses.append(f'"{col}" = :{clean_key}')
                    
                    set_clause = ", ".join(set_clauses)
                    update_query = text(f'UPDATE "{table_name}" SET {set_clause} WHERE rowid = :rowid')
                    
                    # Add rowid to the bind params
                    clean_update_data['rowid'] = rid
                    
                    conn.execute(update_query, clean_update_data)
                    changes_count += 1
            
            trans.commit()
            st.session_state['success_message'] = f"Successfully saved changes! (Added: {len(new_rows)}, Deleted: {len(deleted_ids)}, Updated: {changes_count})"
            st.session_state[f"editor_{table_name}_trigger"] = not st.session_state.get(f"editor_{table_name}_trigger", False)
            
        except Exception as e:
            trans.rollback()
            st.session_state['error_message'] = f"Error saving changes: {e}"

# Sidebar - Table Selection
with st.sidebar:
    st.markdown("---")
    st.subheader("Select Table")
    tables = get_tables(engine)
    if tables:
        selected_table = st.selectbox("Choose a Table", tables)
    else:
        st.warning("No tables found in the database.")
        selected_table = None
    
    st.markdown("---")
    st.markdown("### Usage Instructions")
    st.info("""
    - **Edit**: Double click cells to edit.
    - **Add**: Click the '+' button at the bottom of the table.
    - **Delete**: Select rows and press 'delete' or use the row menu.
    - **Save**: Click 'Save Changes' to apply updates.
    """)

# Main Content
st.title("SQLite Data Editor")

# Display persistent messages
if 'success_message' in st.session_state and st.session_state['success_message']:
    st.success(st.session_state['success_message'])
    st.session_state['success_message'] = None # Clear after showing

if 'error_message' in st.session_state and st.session_state['error_message']:
    st.error(st.session_state['error_message'])
    st.session_state['error_message'] = None # Clear after showing

# Create Tabs
tab_edit, tab_import, tab_sql = st.tabs(["📝 Edit Data", "📥 Import Data", "⚡ SQL Query"])

with tab_edit:
    if selected_table:
        st.subheader(f"Editing Table: `{selected_table}`")
        
        # Load data
        df = load_data(selected_table, engine)
        
        if 'rowid' in df.columns:
            # Configure rowid to be disabled (read-only)
            column_config = {
                "rowid": st.column_config.NumberColumn(
                    "Row ID",
                    help="Internal ID (cannot be edited)",
                    disabled=True,
                    format="%d"
                )
            }
        else:
            column_config = {}

        # Data Editor
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            key=f"editor_{selected_table}",
            width="stretch",
            column_config=column_config,
            hide_index=True # Hide the dataframe index (0,1,2...) to avoid confusion
        )
        
        if st.button("Save Changes", type="primary"):
            save_changes(selected_table, df, edited_df, engine)
            try:
                st.rerun()
            except AttributeError:
                st.experimental_rerun()
    else:
        st.info("Please select a table from the sidebar to start editing.")

with tab_import:
    st.header("Import Data (Excel/CSV)")
    
    # 1. File Uploader
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])
    
    if uploaded_file is not None:
        try:
            # 2. Read file
            if uploaded_file.name.endswith(".csv"):
                import_df = pd.read_csv(uploaded_file)
            else:
                import_df = pd.read_excel(uploaded_file)
            
            st.subheader("Data Preview")
            st.dataframe(import_df.head(10), width="stretch")
            
            st.markdown("---")
            
            # 3. Import Settings
            col1, col2 = st.columns(2)
            with col1:
                # Target table selection (re-use the list from sidebar)
                target_table = st.selectbox("Target Table", tables, key="import_target")
            with col2:
                import_mode = st.radio("Import Mode", ["Append", "Replace"], help="Append adds rows, Replace overwrites the entire table.")

            if st.button("🚀 Start Import"):
                try:
                    with engine.connect() as conn:
                        if_exists_val = "append" if import_mode == "Append" else "replace"
                        
                        # Use to_sql
                        # Note: for 'replace', it will drop and recreate table, which might lose rowids or schema details.
                        # For 'append', it just adds rows.
                        import_df.to_sql(target_table, conn, if_exists=if_exists_val, index=False)
                        conn.commit()
                        
                    st.success(f"Successfully imported {len(import_df)} rows into `{target_table}` ({import_mode}).")
                    st.balloons()
                except Exception as e:
                    st.error(f"Import failed: {e}")
                    
        except Exception as e:
            st.error(f"Error reading file: {e}")

with tab_sql:
    st.header("⚡ Run SQL Query")
    
    query = st.text_area("SQL Query", height=150, placeholder="SELECT * FROM table_name LIMIT 10;")
    
    if st.button("Run Query"):
        if query.strip():
            try:
                with engine.connect() as conn:
                    # check if it's a SELECT query to return dataframe
                    query_upper = query.strip().upper()
                    if query_upper.startswith("SELECT") or query_upper.startswith("WITH") or query_upper.startswith("PRAGMA") or query_upper.startswith("EXPLAIN"):
                        query_df = pd.read_sql(text(query), conn)
                        st.dataframe(query_df, width="stretch")
                    else:
                        # Execute non-select query
                        result = conn.execute(text(query))
                        conn.commit()
                        st.success(f"Query executed successfully. Rows affected: {result.rowcount}")
            except Exception as e:
                st.error(f"Error executing query: {e}")
        else:
            st.warning("Please enter a query.")
