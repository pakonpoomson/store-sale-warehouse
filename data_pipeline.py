import subprocess
import os
import sys # Import sys to check the operating system

def run_multiple_scripts_in_venv(venv_path, script_list):
    """
    รันหลายไฟล์ Python ภายใน Virtual Environment (venv) ตามลำดับ
    (Runs multiple Python files sequentially within a Virtual Environment (venv))

    Args:
        venv_path (str): Path ไปยังโฟลเดอร์ venv (เช่น "my_env") 
                         (Path to the venv folder (e.g., "my_env"))
        script_list (list): รายชื่อไฟล์ Python ที่ต้องการรัน (เช่น ["script1.py", "script2.py"])
                            (List of Python files to run (e.g., ["script1.py", "script2.py"]))

    Returns:
        dict: แสดงผลลัพธ์ stdout และ stderr ของแต่ละสคริปต์
              (Dictionary showing stdout and stderr results for each script)
    """
    # --- Determine the Python Interpreter path based on OS ---
    if sys.platform == "win32": # Check if the OS is Windows
        venv_python = os.path.join(venv_path, "Scripts", "python.exe")
    else: # Assume Linux/macOS or other POSIX systems
        venv_python = os.path.join(venv_path, "bin", "python")
    # ---------------------------------------------------------

    # --- Add a check to ensure the venv Python executable exists ---
    if not os.path.isfile(venv_python):
        print(f"Error: Python interpreter not found at expected path: {venv_python}")
        print("Please ensure the virtual environment path is correct and the environment is properly set up.")
        return None # Return None or raise an exception if the interpreter isn't found
    # -------------------------------------------------------------

    results = {}

    for script_path in script_list:
        # --- Add a check to ensure the script file exists ---
        if not os.path.isfile(script_path):
            print(f"Warning: Script file not found: {script_path}. Skipping.")
            results[script_path] = {"stdout": None, "stderr": f"Error: Script file '{script_path}' not found."}
            continue # Skip to the next script
        # ----------------------------------------------------

        print(f"Executing: {script_path} ...")
        try:
            # รัน Python สคริปต์ (Run the Python script)
            # Use check=False initially to capture errors without raising CalledProcessError immediately
            # We will check the returncode manually
            result = subprocess.run(
                [venv_python, script_path], 
                capture_output=True, 
                text=True, 
                check=False, # Don't raise exception on non-zero exit code automatically
                encoding='utf-8', # Specify encoding for better cross-platform compatibility
                errors='replace'  # Handle potential encoding errors in output
            )

            # Check if the process returned a non-zero exit code (indicating an error)
            if result.returncode != 0:
                 results[script_path] = {"stdout": result.stdout, "stderr": result.stderr if result.stderr else f"Script exited with error code {result.returncode}"}
                 print(f"Error in {script_path}! (Exit code: {result.returncode})\n")
            else:
                 results[script_path] = {"stdout": result.stdout, "stderr": result.stderr if result.stderr else None} # Store stderr even if minor warnings were printed
                 print(f"{script_path} executed successfully!\n")

        except FileNotFoundError as e:
            # This catch is less likely now with the initial venv_python check, but good practice
            results[script_path] = {"stdout": None, "stderr": f"Error executing script: {e}. Is '{venv_python}' correct?"}
            print(f"Failed to start process for {script_path}. Check Python path.\n")
        except Exception as e: 
            # Catch other potential exceptions during subprocess execution
            results[script_path] = {"stdout": None, "stderr": f"An unexpected error occurred: {e}"}
            print(f"An unexpected error occurred while running {script_path}: {e}\n")


    return results


if __name__ == "__main__":
    venv_path = "venv"  # เปลี่ยนเป็น path ของ venv ที่ใช้งาน (Change to the path of your venv)
                         # Make sure this path is correct relative to where data_pipeline.py is run

    # Check if the venv path exists before proceeding
    if not os.path.isdir(venv_path):
         print(f"Error: Virtual environment directory not found at: {venv_path}")
         print("Please ensure the path is correct.")
         exit() # Exit if venv directory doesn't exist

    script_list = [
        "test_ingestion_staging_incremental.py", 
        "test_ingestion_raw_incremental.py"
        ]  # รายชื่อไฟล์ที่ต้องการรัน (List of files to run)

    output = run_multiple_scripts_in_venv(venv_path, script_list)

    if output: # Check if the function returned results (it might return None if venv python wasn't found)
        print("\n--- Execution Summary ---")
        # แสดงผลลัพธ์ของแต่ละสคริปต์ (Display results for each script)
        for script, result in output.items():
            print(f"\n--- Output from {script}: ---")
            if result["stdout"]:
                print("Standard Output:")
                print(result["stdout"])
            else:
                print("Standard Output: (None)")

            if result["stderr"]:
                print("Standard Error:")
                print(result["stderr"])
            else:
                 # Indicate if stderr was empty, differentiating from script not found
                 if "Error: Script file" not in str(result.get("stderr", "")): 
                     print("Standard Error: (None)")
            print(f"--- End of Output for {script} ---")