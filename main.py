from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import pandas as pd

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///well_data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Database Model
class WellData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    api_well_number = db.Column(db.String, unique=True, nullable=False)
    oil = db.Column(db.Float, nullable=False)
    gas = db.Column(db.Float, nullable=False)
    brine = db.Column(db.Float, nullable=False)

# Load data from Excel and calculate annual totals
def load_data_from_excel(file_path):
    data = pd.read_excel(file_path)
    annual_data = data.groupby('API WELL  NUMBER').agg({
        'OIL': 'sum',
        'GAS': 'sum',
        'BRINE': 'sum'
    }).reset_index()
    
    with app.app_context():
        for index, row in annual_data.iterrows():
            well_data = WellData(
                api_well_number=str(row['API WELL  NUMBER']).strip(),  # Convert to string and strip whitespace
                oil=row['OIL'],
                gas=row['GAS'],
                brine=row['BRINE']
            )
            db.session.add(well_data)
        
        db.session.commit()

# API route to get well data
@app.route('/data', methods=['GET'])
def get_well_data():
    api_well_number = request.args.get('well')
    well_data = WellData.query.filter_by(api_well_number=api_well_number.strip()).first()
    
    if well_data:
        return jsonify({
            "oil": well_data.oil,
            "gas": well_data.gas,
            "brine": well_data.brine
        })
    else:
        return jsonify({"error": "Well not found"}), 404

# Main function to create the database and load data
if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create the database tables
        load_data_from_excel('data/excel_fil.xlsx')  
    app.run(port=8080)
