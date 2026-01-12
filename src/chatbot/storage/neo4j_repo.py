from __future__ import annotations
from typing import List, Dict, Any
from chatbot.config.settings import settings

try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None


class Neo4jRepo:
    def __init__(self):
        if GraphDatabase is None:
            raise RuntimeError("neo4j driver not installed.")
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def close(self):
        self.driver.close()
    def get_student_id_by_lti_id(self, lti_id: str) -> str | None:
        query = """
        MATCH (s:Student {lti_id: $lti_id})
        RETURN s.id AS student_id
        """

        with self.driver.session(database=settings.neo4j_database) as session:
            result = session.run(query, lti_id=str(lti_id))
            record = result.single()

            if not record:
                return None

            return record["student_id"]


    def get_module_snippets(self, course_id: str, module_id: str):
        raw_mid = str(module_id)
        mid_plain = raw_mid.replace("module_", "") if raw_mid.startswith("module_") else raw_mid

        query = """
        MATCH (m:Module)-[:has_lesson]->(l:Lesson)
        WHERE (m.id = $mid_raw OR m.id = $mid_plain)
          AND l.data IS NOT NULL AND trim(l.data) <> ""
        RETURN
            l.name AS source,
            l.data AS content,
            l.id   AS page_id,
            l.name AS section
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            result = session.run(query, mid_raw=raw_mid, mid_plain=mid_plain)
            return [
                {
                    "source": r["source"],
                    "content": r["content"] or "",
                    "page_id": r["page_id"],
                    "section": r["section"],
                }
                for r in result
            ]

    def get_competencies_of_module(self, module_id: str) -> List[str]:
        """
        Năng lực ĐẦU RA của module (không phụ thuộc student)
        """
        query = """
        MATCH (m:Module {id:$module_id})-[:HAS_COMPETENCY]->(c:Competency)
        RETURN c.name AS name
        ORDER BY name
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            result = session.run(query, module_id=str(module_id))
            return [r["name"] for r in result]

    def get_student_competencies(self, student_id: str) -> List[Dict[str, Any]]:
        """
        TỔNG năng lực sinh viên đã đạt (toàn hệ thống)
        """
        query = """
        MATCH (s:Student {id:$student_id})-[r:mastery_on]->(m:Module)
        MATCH (m)-[:HAS_COMPETENCY]->(c:Competency)
        RETURN
            c.id   AS competency_id,
            c.name AS competency_name,
            r.mastery AS mastery
        ORDER BY mastery DESC
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            result = session.run(query, student_id=str(student_id))
            return [
                {
                    "id": r["competency_id"],
                    "name": r["competency_name"],
                    "mastery": r["mastery"],
                }
                for r in result
            ]

    def get_achieved_competencies_of_module(
        self, student_id: str, module_id: str
    ) -> List[Dict[str, Any]]:
        """
        Năng lực sinh viên ĐÃ đạt trong module cụ thể
        """
        query = """
        MATCH (s:Student {id:$student_id})-[r:mastery_on]->(m:Module {id:$module_id})
        MATCH (m)-[:HAS_COMPETENCY]->(c:Competency)
        RETURN c.name AS name, r.mastery AS mastery
        ORDER BY r.mastery DESC
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            result = session.run(
                query,
                student_id=str(student_id),
                module_id=str(module_id),
            )
            return [dict(r) for r in result]
