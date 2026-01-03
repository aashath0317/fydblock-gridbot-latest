import sys
from unittest.mock import patch

import pytest

from utils.arg_parser import parse_and_validate_console_args


@pytest.mark.parametrize(
    ("args", "expected_config"),
    [
        (["--config", "config1.json"], ["config1.json"]),
        (["--config", "config1.json", "config2.json"], ["config1.json", "config2.json"]),
    ],
)
@patch("os.path.exists", return_value=True)  # Mock os.path.exists to always return True
def test_parse_and_validate_console_args_required(mock_exists, args, expected_config):
    with patch.object(sys, "argv", ["program_name", *args]):
        result = parse_and_validate_console_args()
        assert result.config == expected_config, f"Expected {expected_config}, got {result.config}"


@patch("os.path.exists", return_value=True)
def test_parse_and_validate_console_args_save_performance_results_exists(mock_exists):
    with patch.object(
        sys,
        "argv",
        ["program_name", "--config", "config.json", "--save_performance_results", "results.json"],
    ):
        result = parse_and_validate_console_args()
        assert result.save_performance_results == "results.json"


def test_parse_and_validate_console_args_save_performance_results_dir_does_not_exist():
    with (
        patch("os.path.exists", side_effect=lambda path: path == "config.json"),
        patch.object(
            sys,
            "argv",
            ["program_name", "--config", "config.json", "--save_performance_results", "non_existent_dir/results.json"],
        ),
        patch("utils.arg_parser.logging.error") as mock_log,
    ):
        with pytest.raises(RuntimeError, match="Argument validation failed."):
            parse_and_validate_console_args()
        mock_log.assert_called_once_with(
            "Validation failed: The directory for saving performance results does not exist: non_existent_dir",
        )


@patch("os.path.exists", return_value=True)
def test_parse_and_validate_console_args_no_plot(mock_exists):
    with patch.object(sys, "argv", ["program_name", "--config", "config.json", "--no-plot"]):
        result = parse_and_validate_console_args()

        assert hasattr(result, "no_plot"), "The `no_plot` attribute is missing from the parsed result."
        assert result.no_plot is True, "The `no_plot` flag was not set to True."


@patch("os.path.exists", return_value=True)
def test_parse_and_validate_console_args_profile(mock_exists):
    with patch.object(sys, "argv", ["program_name", "--config", "config.json", "--profile"]):
        result = parse_and_validate_console_args()

        assert hasattr(result, "profile"), "The `profile` attribute is missing from the parsed result."
        assert result.profile is True, "The `profile` flag was not set to True."


@patch("utils.arg_parser.logging.error")
def test_parse_and_validate_console_args_argument_error(mock_log):
    with patch.object(sys, "argv", ["program_name", "--config"]):
        with pytest.raises(RuntimeError, match="Failed to parse arguments. Please check your inputs."):
            parse_and_validate_console_args()
        mock_log.assert_called_once_with("Argument parsing failed: 2")


@patch("utils.arg_parser.logging.error")
def test_parse_and_validate_console_args_unexpected_error(mock_log):
    with (
        patch.object(
            sys,
            "argv",
            ["program_name", "--config", "config.json", "--save_performance_results", "results.json"],
        ),
        patch("os.path.exists", side_effect=Exception("Unexpected error")),
    ):
        with pytest.raises(RuntimeError, match="An unexpected error occurred during argument parsing."):
            parse_and_validate_console_args()
        mock_log.assert_any_call("An unexpected error occurred while parsing arguments: Unexpected error")
