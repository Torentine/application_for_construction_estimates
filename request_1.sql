-- для работ

SELECT 
    w.name_work AS work_name,
    COUNT(*) AS total_occurrences,
    COUNT(DISTINCT le.id) AS distinct_estimates_count
FROM 
    work w
JOIN 
    sections s ON w.local_section_id = s.id
JOIN 
    local_estimates le ON s.estimate_id = le.id
GROUP BY 
    w.name_work
ORDER BY 
    total_occurrences DESC;


-- для матер.

SELECT 
    m.name_material AS material_name,
    COUNT(*) AS total_occurrences,
    COUNT(DISTINCT le.id) AS distinct_estimates_count
FROM 
    materials m
JOIN 
    work w ON m.work_id = w.id
JOIN 
    sections s ON w.local_section_id = s.id
JOIN 
    local_estimates le ON s.estimate_id = le.id
GROUP BY 
    m.name_material
ORDER BY 
    total_occurrences DESC;