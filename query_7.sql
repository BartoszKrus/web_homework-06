SELECT students.id, students.name, grades.grade, grades.date
FROM students
JOIN groups ON students.group_id = groups.id
JOIN grades ON students.id = grades.student_id
JOIN subjects ON grades.subject_id = subjects.id
WHERE groups.name = ? AND subjects.name = ?;
