--+-------------+---------+
--| Column Name | Type    |
--+-------------+---------+
--| student_id  | int     |
--| subject     | varchar |
--| score       | int     |
--| exam_date   | varchar |
--+-------------+---------+
--(student_id, subject, exam_date) is the primary key for this table.
--Each row contains information about a student's score in a specific subject on a particular exam date. score is between 0 and 100 (inclusive).
--Write a solution to find the students who have shown improvement. A student is considered to have shown improvement if they meet both of these conditions:
--•	Have taken exams in the same subject on at least two different dates
--•	Their latest score in that subject is higher than their first score

-- SOLUTION:
WITH ranked_scores AS (
    SELECT 
        student_id,
        subject,
        score,
        exam_date,
        FIRST_VALUE(score) OVER (PARTITION BY student_id, subject ORDER BY exam_date) AS first_score,
        LAST_VALUE(score) OVER (PARTITION BY student_id, subject ORDER BY exam_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_score,
        COUNT(*) OVER (PARTITION BY student_id, subject) AS exam_count
    FROM Scores
)
SELECT DISTINCT
    student_id,
    subject,
    first_score,
    latest_score
FROM ranked_scores
WHERE exam_count >= 2
  AND latest_score > first_score
ORDER BY student_id, subject;

--________________________________________
--✅ Goal:
--Identify students who:
--1.	Took at least two exams in the same subject.
--2.	Improved their score from the first exam to the latest exam (based on exam_date).
________________________________________
--🔧 Step-by-Step Code Breakdown:
--1. WITH ranked_scores AS (...)
--This is a Common Table Expression (CTE). Think of it as a temporary table to organize the data before filtering.
--________________________________________
--2. Inside the CTE:

--SELECT 
--    student_id,
--    subject,
--    score,
--    exam_date,
--We retrieve the basic columns for analysis.
--________________________________________
--3. FIRST_VALUE(score) OVER (...) AS first_score
--sql
--CopyEdit
--FIRST_VALUE(score) OVER (
--    PARTITION BY student_id, subject 
--    ORDER BY exam_date
--) AS first_score,
--•	PARTITION BY student_id, subject: Group the data by each student and subject.
--•	ORDER BY exam_date: Within each group, order exams chronologically.
--•	FIRST_VALUE(score): picks the earliest exam score per student & subject.
--________________________________________
--4. LAST_VALUE(score) OVER (...) AS latest_score
--sql
--CopyEdit
--LAST_VALUE(score) OVER (
--    PARTITION BY student_id, subject 
--    ORDER BY exam_date 
--    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--) AS latest_score,
--•	LAST_VALUE(score): picks the latest exam score.
--•	BUT: by default, LAST_VALUE returns the current row’s value unless we define the window.
--•	So we add:

--ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--to force the window to look at the entire partition, ensuring we get the actual last score.
--________________________________________
--5. COUNT(*) OVER (...) AS exam_count

--CopyEdit
--COUNT(*) OVER (PARTITION BY student_id, subject) AS exam_count
--•	Counts how many exams each student took for each subject.
--•	We only want students who took 2 or more.
--_______________________________________
--6. Final Filtering and Output:

--SELECT DISTINCT
--    student_id,
--    subject,
--    first_score,
--   latest_score
--FROM ranked_scores
--WHERE exam_count >= 2
--  AND latest_score > first_score
--ORDER BY student_id, subject;
--•	exam_count >= 2: ensures at least two attempts.
--•	latest_score > first_score: checks for improvement.
--•	DISTINCT: avoids duplicate rows since the CTE outputs 1 row per exam.
--•	ORDER BY student_id, subject: formats the result as required.