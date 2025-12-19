import psycopg2
import csv
import os
output_folder = os.path.join("data", "triples")


DB_NAME = "canvas_production"     
DB_USER = "canvas"                
DB_PASS = "PhucThanh2002"         
DB_HOST = "172.29.96.167"             
DB_PORT = 5432                    
SQL = """
SELECT 
    u.id AS user_id,
    u.name,
    u.lti_id
FROM users u
ORDER BY u.id;
"""
def export_canvas_user_lti():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )

        cur = conn.cursor()
        cur.execute(SQL)
        rows = cur.fetchall()

        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, "canvas_user_lti_export.csv")

        with open(output_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "name", "lti_id"])
            for user_id, name, lti_id in rows:
                user_str = f"user_{user_id}"      
                writer.writerow([user_str, name, lti_id])

    except Exception as e:
        print("Lỗi khi truy vấn:", e)

    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    export_canvas_user_lti()
