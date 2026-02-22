import csv
import os
from models import Student


# ── CSV I/O ─────────────────────────────────────────────────────────────


def load_students_from_csv(filepath: str) -> list[Student]:
    students: list[Student] = []
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                try:
                    student = Student.from_dict(row)
                    students.append(student)
                except (ValueError, KeyError) as e:
                    print(f"  ⚠ Skipping row {row_num}: {e}")
    except FileNotFoundError:
        print(f"  ✗ File not found: {filepath}")
    except PermissionError:
        print(f"  ✗ Permission denied: {filepath}")
    except Exception as e:
        print(f"  ✗ Error reading file: {e}")

    return students


def save_students_to_csv(students: list[Student], filepath: str) -> bool:
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            max_grades = max((len(s.grades) for s in students), default=0)
            fieldnames = ["student_id", "name"] + [
                f"grade{i+1}" for i in range(max_grades)
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for s in students:
                row: dict = {"student_id": s.student_id, "name": s.name}
                for i, g in enumerate(s.grades):
                    row[f"grade{i+1}"] = g
                writer.writerow(row)

        return True
    except PermissionError:
        print(f"  ✗ Permission denied: {filepath}")
    except Exception as e:
        print(f"  ✗ Error saving file: {e}")
    return False


# ── Input Validation ────────────────────────────────────────────────────


def validate_input(prompt: str, type_ = str, range_: tuple | None = None):
    while True:
        raw = input(prompt).strip()
        if not raw:
            print("  ✗ Input cannot be empty. Try again.")
            continue
        try:
            value = type_(raw)
        except (ValueError, TypeError):
            print(f"  ✗ Please enter a valid {type_.__name__}.")
            continue

        if range_ is not None and isinstance(value, (int, float)):
            lo, hi = range_
            if not (lo <= value <= hi):
                print(f"  ✗ Value must be between {lo} and {hi}.")
                continue
        return value


# ── Formatting Helpers ──────────────────────────────────────────────────


def print_separator(char: str = "─", length: int = 60) -> None:
    print(char * length)


def print_header(title: str) -> None:
    print_separator()
    print(f"  {title}")
    print_separator()


def get_data_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.csv")
