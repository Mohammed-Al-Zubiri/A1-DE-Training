from models import Student, Classroom
from analytics import top_performer, lowest_performer, rank_students, grade_distribution
from utils import (
    save_students_to_csv,
    validate_input,
    print_separator,
    print_header,
    get_data_path,
)


# ── Menu Helpers ────────────────────────────────────────────────────────


def show_menu() -> None:
    print_header("Student Performance Analyzer")
    print("  1 │ View all students")
    print("  2 │ Add a student")
    print("  3 │ Remove a student")
    print("  4 │ Search for a student")
    print("  5 │ View analytics")
    print("  6 │ Save & Exit")
    print_separator()


# ── Menu Actions ────────────────────────────────────────────────────────


def view_all_students(classroom: Classroom) -> None:
    print_header("All Students")
    if classroom.size == 0:
        print("  (no students loaded)")
        return
    for s in classroom.students:
        print(f"  {s}")
    print_separator()
    print(f"  Total: {classroom.size} students")


def add_student(classroom: Classroom) -> None:
    print_header("Add New Student")
    try:
        sid = validate_input("  Enter student ID : ", int, (1, 99999))
        name = validate_input("  Enter student name: ", str)
        num = validate_input("  How many grades?  : ", int, (1, 20))

        grades: list[float] = []
        for i in range(num):
            g = validate_input(f"    Grade {i+1}: ", float, (0, 100))
            grades.append(g)

        student = Student(sid, name, grades)
        classroom.add_student(student)
        print(f"\n  ✓ Added: {student}")
    except ValueError as e:
        print(f"\n  ✗ Could not add student: {e}")


def remove_student(classroom: Classroom) -> None:
    print_header("Remove Student")
    try:
        sid = validate_input("  Enter student ID to remove: ", int)
        removed = classroom.remove_student(sid)
        print(f"\n  ✓ Removed: {removed}")
    except KeyError as e:
        print(f"\n  ✗ {e}")


def search_student(classroom: Classroom) -> None:
    print_header("Search Student")
    query = validate_input("  Enter name or ID: ", str)
    results = classroom.search_student(query)
    if results:
        print(f"\n  Found {len(results)} result(s):")
        for s in results:
            print(f"    {s}")
    else:
        print("\n  ✗ No students matched your query.")


def view_analytics(classroom: Classroom) -> None:
    print_header("Analytics Report")

    students = classroom.students
    if not students:
        print("  (no students to analyze)")
        return

    # Class average
    print(f"\n  Class Average: {classroom.classroom_average():.1f}")

    # Top performer
    top = top_performer(students)
    if top:
        print(f"  Top Performer: {top.name} (Avg: {top.calculate_average():.1f})")

    # Lowest performer
    low = lowest_performer(students)
    if low:
        print(f"  Lowest Performer: {low.name} (Avg: {low.calculate_average():.1f})")

    # Grade distribution
    dist = grade_distribution(students)
    print("\n  Grade Distribution:")
    for grade_letter in ("A", "B", "C", "D", "F"):
        count = dist[grade_letter]
        bar = "█" * count
        print(f"    {grade_letter}: {bar} ({count})")

    # Full ranking
    ranked = rank_students(students)
    print("\n  Student Ranking:")
    print(f"  {'Rank':<6}{'Name':<22}{'Average':<10}{'Grade'}")
    print("  " + "─" * 48)
    for rank, s in enumerate(ranked, start=1):
        avg = s.calculate_average()
        print(f"  {rank:<6}{s.name:<22}{avg:<10.1f}{s.grade_category()}")


# ── Main Loop ───────────────────────────────────────────────────────────


def main() -> None:
    data_path = get_data_path()

    # Load classroom from CSV
    print("\n  Loading students from data.csv ...")
    try:
        classroom = Classroom.from_csv(data_path)
        print(f"  ✓ Loaded {classroom.size} students.\n")
    except Exception as e:
        print(f"  ✗ Failed to load data: {e}")
        classroom = Classroom()

    while True:
        show_menu()
        choice = validate_input("  Choose an option (1-6): ", int, (1, 6))

        if choice == 1:
            view_all_students(classroom)
        elif choice == 2:
            add_student(classroom)
        elif choice == 3:
            remove_student(classroom)
        elif choice == 4:
            search_student(classroom)
        elif choice == 5:
            view_analytics(classroom)
        elif choice == 6:
            # Save & Exit
            print("\n  Saving students ...")
            if save_students_to_csv(classroom.students, data_path):
                print("  ✓ Data saved successfully.")
            else:
                print("  ✗ Save failed — see errors above.")
            print("  Goodbye!\n")
            break


if __name__ == "__main__":
    main()
