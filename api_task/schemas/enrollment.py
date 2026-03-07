from pydantic import BaseModel
from typing import Optional


class EnrollmentBase(BaseModel):
    student_id: int
    course_id: int
    status: Optional[str] = "enrolled"


class EnrollmentCreate(EnrollmentBase):
    pass


class EnrollmentUpdate(BaseModel):
    status: Optional[str] = None


class EnrollmentResponse(EnrollmentBase):
    id: int

    class Config:
        from_attributes = True
