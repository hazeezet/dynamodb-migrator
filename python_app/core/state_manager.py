import json
import sys
import logging
from .config import STATE_FILE, UNDO_FILE
from .utils.logger import get_logger

logger = get_logger()


def load_state():
    """Load migration state from file."""

    try:

        with open(STATE_FILE, "r") as f:

            return json.load(f)

    except FileNotFoundError:

        return {"migrations": []}

    except json.JSONDecodeError:

        logger.error("State file is corrupted.")

        print("State file is corrupted. Please fix or delete 'migration_state.json'.")

        sys.exit(1)

    except Exception as e:

        logger.error(f"Error loading state: {e}")

        print(f"Error loading state: {e}")

        sys.exit(1)


def save_state(state):
    """Save migration state to file."""

    try:

        with open(STATE_FILE, "w") as f:

            json.dump(state, f, indent=4)

    except Exception as e:

        logger.error(f"Error saving state: {e}")

        print(f"Error saving state: {e}")

        sys.exit(1)


def load_undo_state():
    """Load undo state from file."""

    try:

        with open(UNDO_FILE, "r") as f:

            return json.load(f)

    except FileNotFoundError:

        return {"undo_migrations": {}}

    except json.JSONDecodeError:

        logger.error("Undo file is corrupted.")

        print("Undo file is corrupted. Please fix or delete 'undo_state.json'.")

        sys.exit(1)

    except Exception as e:

        logger.error(f"Error loading undo state: {e}")

        print(f"Error loading undo state: {e}")

        sys.exit(1)


def save_undo_state(undo_state):
    """Save undo state to file."""

    try:

        with open(UNDO_FILE, "w") as f:

            json.dump(undo_state, f, indent=4)

    except Exception as e:

        logger.error(f"Error saving undo state: {e}")

        print(f"Error saving undo state: {e}")

        sys.exit(1)
