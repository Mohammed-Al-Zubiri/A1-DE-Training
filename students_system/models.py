class Student:
    
    def __init__(self, student_id: int, name: str, grades: list[float]) -> None:
        self._student_id = student_id
        self._name = name
        self._grades = [g for g in grades if self.validate_grade(g)]

    # ── Properties (encapsulation) ──────────────────────────────────────

    @property
    def student_id(self) -> int:
                return self._student_id

    @property
    def name(self) -> str:
                return self._name

    @name.setter
    def name(self, value: str) -> None:
        if not value or not value.strip():
            raise ValueError("Name cannot be empty.")
        self._name = value.strip()

    @property
    def grades(self) -> list[float]:
                return list(self._grades)

    # ── Instance methods ────────────────────────────────────────────────

    def calculate_average(self) -> float:
        if not self._grades:
            return 0.0
        return sum(self._grades) / len(self._grades)

    def grade_category(self) -> str:
        avg = self.calculate_average()
        if avg >= 90:
            return "A"
        elif avg >= 80:
            return "B"
        elif avg >= 70:
            return "C"
        elif avg >= 60:
            return "D"
        else:
            return "F"

    def add_grade(self, grade: float) -> None:
        if not self.validate_grade(grade):
            raise ValueError(f"Invalid grade: {grade}. Must be between 0 and 100.")
        self._grades.append(grade)

    # ── Class methods ───────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, data: dict) -> "Student":
        student_id = int(data["student_id"])
        name = data["name"]
        grades: list[float] = []
        for key, value in data.items():
            if key.startswith("grade"):
                try:
                    grades.append(float(value))
                except (ValueError, TypeError):
                    pass  # skip non-numeric grade columns
        return cls(student_id, name, grades)

    # ── Static methods ──────────────────────────────────────────────────

    @staticmethod
    def validate_grade(grade) -> bool:
        try:
            grade = float(grade)
            return 0 <= grade <= 100
        except (ValueError, TypeError):
            return False

    # ── Magic methods ───────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"Student(id={self._student_id}, name='{self._name}', "
            f"grades={self._grades})"
        )

    def __str__(self) -> str:
        avg = self.calculate_average()
        return (
            f"[{self._student_id}] {self._name:20s} | "
            f"Grades: {self._grades} | Avg: {avg:.1f} | {self.grade_category()}"
        )


class Classroom:
    
    def __init__(self) -> None:
        self._students: list[Student] = []

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def students(self) -> list[Student]:
        return list(self._students)

    @property
    def size(self) -> int:
        return len(self._students)

    # ── Instance methods ────────────────────────────────────────────────

    def add_student(self, student: Student) -> None:
        if any(s.student_id == student.student_id for s in self._students):
            raise ValueError(
                f"Student with ID {student.student_id} already exists."
            )
        self._students.append(student)

    def remove_student(self, student_id: int) -> Student:
        for i, s in enumerate(self._students):
            if s.student_id == student_id:
                return self._students.pop(i)
        raise KeyError(f"No student found with ID {student_id}.")

    def search_student(self, query: str) -> list[Student]:
        results: list[Student] = []
        query_lower = query.strip().lower()
        for s in self._students:
            if query_lower in s.name.lower() or query_lower == str(s.student_id):
                results.append(s)
        return results

    def classroom_average(self) -> float:
        if not self._students:
            return 0.0
        total = sum(s.calculate_average() for s in self._students)
        return total / len(self._students)

    # ── Class methods ───────────────────────────────────────────────────

    @classmethod
    def from_csv(cls, filepath: str) -> "Classroom":
        from utils import load_students_from_csv

        classroom = cls()
        students = load_students_from_csv(filepath)
        for student in students:
            classroom.add_student(student)
        return classroom

    # ── Magic methods ───────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"Classroom(size={self.size})"

    def __str__(self) -> str:
        header = f"Classroom — {self.size} students"
        lines = [header, "=" * len(header)]
        for s in self._students:
            lines.append(str(s))
        return "\n".join(lines)
