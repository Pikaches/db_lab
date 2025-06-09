from flask import Flask, request, jsonify
from const import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

from session_type_search import SessionTypeSearch
from lab import AttendanceFinder
from lecture_session import LectureMaterialSearcher


app = Flask(__name__)


def has_all_required_fields(data, required_fields):
    if not all(field in data for field in required_fields):
        return False
    return True


@app.route('/api/lab1/report', methods=['POST'])
def generate_attendance_report():
    if not request.is_json:
        return jsonify({'error': 'Request must be JSON'}), 400

    data = request.get_json()

    required_fields = ['name', 'start_date', 'end_date']
    if not has_all_required_fields(data, required_fields):
        jsonify({
            'error': f"Missing required fields: {required_fields}",
            'received': list(data.keys())
        }), 400

    # Find All Lectures

    # Filter lectures with type 'Лекция'
    session_searcher = SessionTypeSearch()
    sessions_id = session_searcher.get_by_name('Лекция')

    es_searcher = LectureMaterialSearcher()
    lecture_sessions_ids = es_searcher.search_by_course_and_session_type(
        data['name'], sessions_id[0]['id'])

    if not lecture_sessions_ids:
        return jsonify({'error': 'No lectures found for the name'}), 404

    finder = AttendanceFinder(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD
    )

    try:
        worst = finder.find_worst_attendees(
            lecture_sessions_ids,
            top_n=10,
            start_date=data['start_date'],
            end_date=data['end_date']
        )

        print(worst)

        report = {
            'search_term': data['name'],
            'period': f"{data['start_date']} - {data['end_date']}",
            'found_lectures': len(lecture_sessions_ids),
            'worst_attendees': [r for r in worst]
        }
        return jsonify(report=report, meta={'status': 'success', 'results': len(worst)}), 200

    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({'error': 'Data processing failed'}), 500

    finally:
        finder.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
