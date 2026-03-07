from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

import models.enrollment as models
import schemas.enrollment as schemas
from database import get_db

router = APIRouter(prefix="/enrollments", tags=["Enrollments"])


@router.post("/", response_model=schemas.EnrollmentResponse)
def create_enrollment(
    enrollment: schemas.EnrollmentCreate, db: Session = Depends(get_db)
):
    db_enrollment = models.Enrollment(**enrollment.dict())
    db.add(db_enrollment)
    db.commit()
    db.refresh(db_enrollment)
    return db_enrollment


@router.get("/", response_model=List[schemas.EnrollmentResponse])
def read_enrollments(db: Session = Depends(get_db)):
    return db.query(models.Enrollment).all()


@router.get("/{enrollment_id}", response_model=schemas.EnrollmentResponse)
def read_enrollment(enrollment_id: int, db: Session = Depends(get_db)):
    enrollment = (
        db.query(models.Enrollment)
        .filter(models.Enrollment.id == enrollment_id)
        .first()
    )
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    return enrollment


@router.put("/{enrollment_id}", response_model=schemas.EnrollmentResponse)
def update_enrollment(
    enrollment_id: int,
    enrollment: schemas.EnrollmentUpdate,
    db: Session = Depends(get_db),
):
    db_enrollment = (
        db.query(models.Enrollment)
        .filter(models.Enrollment.id == enrollment_id)
        .first()
    )
    if not db_enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    for key, value in enrollment.dict(exclude_unset=True).items():
        setattr(db_enrollment, key, value)
    db.commit()
    db.refresh(db_enrollment)
    return db_enrollment


@router.delete("/{enrollment_id}")
def delete_enrollment(enrollment_id: int, db: Session = Depends(get_db)):
    db_enrollment = (
        db.query(models.Enrollment)
        .filter(models.Enrollment.id == enrollment_id)
        .first()
    )
    if not db_enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    db.delete(db_enrollment)
    db.commit()
    return {"detail": "Enrollment deleted"}
