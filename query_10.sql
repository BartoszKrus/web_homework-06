SELECT subjects.id, subjects.name
FROM subjects
JOIN lecturers ON subjects.lecturer_id = lecturers.id
JOIN grades ON subjects.id = grades.subject_id
JOIN students ON grades.student_id = students.id
WHERE lecturers.name = ? AND students.name = ?
GROUP BY subjects.id, subjects.name;
