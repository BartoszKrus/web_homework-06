SELECT students.id, students.name, AVG(grades.grade) as average_grade
FROM students
JOIN grades ON students.id = grades.student_id
JOIN subjects ON grades.subject_id = subjects.id
WHERE subjects.name = ?
GROUP BY students.id, students.name
ORDER BY average_grade DESC
LIMIT 1;
