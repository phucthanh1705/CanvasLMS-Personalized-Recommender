from neo4j import GraphDatabase
from chatbot.core.module_nlp_resolver import (
    resolve_module_by_explicit_subject,
    resolve_module_by_lesson_blocks_topk,
)
from chatbot.config.settings import settings


class IDResolver:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def _normalize_module_id(self, module_id: str) -> str:
        """
        Normalize module id:
        - module_59 -> 59
        - 59 -> 59
        """
        if not module_id:
            return module_id
        module_id = str(module_id)
        if module_id.startswith("module_"):
            return module_id.replace("module_", "")
        return module_id

    def close(self):
        self.driver.close()

    # ===============================
    # COURSE
    # ===============================
    def resolve_course_id(self, external_course_id: str) -> str | None:
        query = """
        MATCH (c:Course {id: $course_id})
        RETURN c.id AS id
        LIMIT 1
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            record = session.run(query, course_id=external_course_id).single()
            return record["id"] if record else None

    # ===============================
    # MODULE (DIRECT)
    # ===============================
    def resolve_module_id(self, course_id: str, external_module_id: str) -> str | None:
        external_module_id = self._normalize_module_id(external_module_id)

        query = """
        MATCH (c:Course {id: $course_id})-[:HAS_MODULE]->(m:Module {id: $module_id})
        RETURN m.id AS id
        LIMIT 1
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            record = session.run(
                query,
                course_id=course_id,
                module_id=external_module_id
            ).single()
            return record["id"] if record else None


    def resolve_student(self, student_id: str) -> bool:
        query = """
        MATCH (s:Student {id: $student_id})
        RETURN s.id
        LIMIT 1
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            return session.run(query, student_id=student_id).single() is not None

    def resolve_module_from_question_nlp(self, course_id: str, question: str) -> str | None:
        module_query = """
        MATCH (c:Course {id: $course_id})-[:includes]->(m:Module)
        WHERE m.subject_name IS NOT NULL AND trim(m.subject_name) <> ""
        RETURN
            m.id AS module_id,
            m.subject_name AS subject_name
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            module_rows = [
                {
                    "module_id": r["module_id"],
                    "subject_name": r["subject_name"]
                }
                for r in session.run(
                    module_query,
                    course_id=course_id
                )
            ]
        
        mid = resolve_module_by_explicit_subject(question, module_rows)
        if mid:
            mid = self._normalize_module_id(mid)
            print("NLP explicit subject →", mid)
            return mid

        lesson_query = """
        MATCH (c:Course {id: $course_id})
              -[:HAS_MODULE]->(m:Module)
              -[:has_lesson]->(l:Lesson)
        WHERE l.data IS NOT NULL AND trim(l.data) <> ""
        RETURN m.id AS module_id, l.data AS content
        """
        with self.driver.session(database=settings.neo4j_database) as session:
            lesson_rows = [
                {
                    "module_id": self._normalize_module_id(r["module_id"]),
                    "content": r["content"]
                }
                for r in session.run(
                    lesson_query,
                    course_id=course_id
                )
            ]

        if not lesson_rows:
            print("NLP: no lesson content found")
            return None

        mid = resolve_module_by_lesson_blocks_topk(
        question=question,
        lesson_rows=lesson_rows,
        threshold=0.23,                 # câu hỏi khái niệm: hạ nhẹ
        top_k_blocks_per_module=6,
        max_chars_block=350
    )


        if mid:
            mid = self._normalize_module_id(mid)
            print("NLP embedding →", mid)
            return mid

        print("NLP failed to resolve module")
        return None
