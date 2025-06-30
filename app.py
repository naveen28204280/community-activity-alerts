import pymysql
import configparser

# ... keep everything else as-is ...

def get_db_connection():
    cfg = configparser.ConfigParser()
    cfg.read('/data/project/community-activity-alerts-system/replica.my.cnf')
    user = cfg['client']['user']
    password = cfg['client']['password']

    conn = pymysql.connect(
        host='tools.db.svc.wikimedia.cloud',
        user=user,
        password=password,
        database='s56391__community_alerts',
        charset='utf8mb4'
    )
    return conn

@app.route('/')
def index():
    language = request.args.get('language')
    project_group = request.args.get('project_group')
    datestart = request.args.get('datestart')
    dateend = request.args.get('dateend')
    filter_edits = request.args.get('filter_edits') == 'true'
    filter_users = request.args.get('filter_users') == 'true'

    if not (language and project_group and datestart and dateend):
        return render_template('index.html', languages=get_all_communities())

    project = project_group.split(":/")[1][1:]  # e.g. "en.wikipedia.org"
    start = datetime.strptime(datestart, "%b %Y")
    end = datetime.strptime(dateend, "%b %Y")

    try:
        conn = get_db_connection()
        query = """
            SELECT timestamp, edit_count AS edits
            FROM edit_counts
            WHERE project = %s
              AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn, params=(project, start, end))
        conn.close()

        if df.empty:
            return render_template('index.html', languages=get_all_communities(), data=[], message="No data available.")

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        peaks = find_peaks_rolling_3_years(df, threshold_percentage=0.30)
        peaks = log_peaks(peaks)

        return render_template('index.html', languages=get_all_communities(), data=peaks)

    except Exception as e:
        return f"Database error: {str(e)}"
