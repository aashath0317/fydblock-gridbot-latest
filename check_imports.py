import os
import sys
import importlib
import pkgutil

# Add project root
sys.path.append(os.getcwd())


def check_imports(start_dir):
    print(f"Checking imports in {start_dir}...")
    errors = []

    # Walk through the directory
    for root, dirs, files in os.walk(start_dir):
        if "venv" in root or "__pycache__" in root:
            continue

        for file in files:
            if file.endswith(".py"):
                # Construct module name
                rel_path = os.path.relpath(os.path.join(root, file), start_dir)
                module_name = rel_path.replace(os.sep, ".").replace(".py", "")

                try:
                    importlib.import_module(module_name)
                    # print(f"✅ Imported {module_name}")
                except Exception as e:
                    print(f"❌ Failed to import {module_name}: {e}")
                    errors.append((module_name, str(e)))

    if errors:
        print("\n--- Import Errors Found ---")
        for mod, err in errors:
            print(f"{mod}: {err}")
        sys.exit(1)
    else:
        print("\n🎉 All modules imported successfully!")


if __name__ == "__main__":
    check_imports(".")
