SELECT subjects.id, subjects.name
FROM subjects
JOIN grades ON subjects.id = grades.subject_id
JOIN students ON grades.student_id = students.id
WHERE students.name = ?
GROUP BY subjects.id, subjects.name;
