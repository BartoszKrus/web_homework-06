SELECT subjects.id, subjects.name
FROM subjects
JOIN lecturers ON subjects.lecturer_id = lecturers.id
WHERE lecturers.name = ?;
