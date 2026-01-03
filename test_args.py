import sys
import os

# Check if we can verify the arg fix
# Add project root
sys.path.append(os.getcwd())

from utils.arg_parser import parse_and_validate_console_args


def test_no_args():
    print("Testing no args...")
    # Simulate no args passed
    # We pass empty list to simulate no CLI args given to script
    try:
        args = parse_and_validate_console_args([])
        print(f"Success! Args: {args}")
        if args.config and "config/config.json" in args.config[0]:
            print("Correctly defaulted to config/config.json")
        else:
            print(f"Unexpected config value: {args.config}")
    except Exception as e:
        print(f"FAILED: {e}")


if __name__ == "__main__":
    test_no_args()
