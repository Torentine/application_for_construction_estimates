DROP TABLE IF EXISTS "objects";
CREATE TABLE "objects" (
  "id" SERIAL PRIMARY KEY,
  "object_name" VARCHAR(1000) NOT NULL
);

DROP TABLE IF EXISTS "local_estimates";
CREATE TABLE "local_estimates" (
  "id" SERIAL PRIMARY KEY,
  "object_estimates_id" INT NOT NULL,
  "name_local_estimate" VARCHAR(1000) NOT NULL,
  "local_estimates_price" DECIMAL(12, 2) 
);

DROP TABLE IF EXISTS "sections";
CREATE TABLE "sections" (
  "id" SERIAL PRIMARY KEY,
  "estimate_id" INT NOT NULL,
  "name_section" VARCHAR(1000) NOT NULL
);

DROP TABLE IF EXISTS "work";
CREATE TABLE "work" (
  "id" SERIAL PRIMARY KEY,
  "local_section_id" INT NOT NULL,
  "name_work" VARCHAR(1000) NOT NULL,
  "price" DECIMAL(12, 2) NOT NULL,
  "measurement_unit" VARCHAR(250) NOT NULL
);

DROP TABLE IF EXISTS "materials";
CREATE TABLE "materials" (
  "id" SERIAL PRIMARY KEY,
  "work_id" INT NOT NULL,
  "name_material" VARCHAR(1000) NOT NULL,
  "price" DECIMAL(12, 2) NOT NULL,
  "measurement_unit" VARCHAR(250) NOT NULL
);

DROP TABLE IF EXISTS "object_estimates";
CREATE TABLE "object_estimates" (
  "id" SERIAL PRIMARY KEY,
  "object_id" INT NOT NULL,
  "name_object_estimate" VARCHAR(1000) NOT NULL,
  "object_estimates_price" DECIMAL(12, 2) NOT NULL
);

ALTER TABLE "local_estimates" 
ADD FOREIGN KEY ("object_estimates_id") 
REFERENCES "object_estimates"("id") 
ON DELETE CASCADE;

ALTER TABLE "sections" 
ADD FOREIGN KEY ("estimate_id") 
REFERENCES "local_estimates"("id") 
ON DELETE CASCADE;

ALTER TABLE "work" 
ADD FOREIGN KEY ("local_section_id") 
REFERENCES "sections"("id") 
ON DELETE CASCADE;

ALTER TABLE "materials" 
ADD FOREIGN KEY ("work_id") 
REFERENCES "work"("id") 
ON DELETE CASCADE;

ALTER TABLE "object_estimates" 
ADD FOREIGN KEY ("object_id") 
REFERENCES "objects"("id") 
ON DELETE CASCADE;