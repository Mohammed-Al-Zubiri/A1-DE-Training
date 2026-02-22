from models import Student


def top_performer(students: list[Student]) -> Student | None:
    if not students:
        return None
    return max(students, key=lambda s: s.calculate_average())


def lowest_performer(students: list[Student]) -> Student | None:
    if not students:
        return None
    return min(students, key=lambda s: s.calculate_average())


def rank_students(students: list[Student]) -> list[Student]:
    return sorted(students, key=lambda s: s.calculate_average(), reverse=True)


def grade_distribution(students: list[Student]) -> dict[str, int]:
    distribution: dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
    for s in students:
        category = s.grade_category()
        distribution[category] = distribution.get(category, 0) + 1
    return distribution
