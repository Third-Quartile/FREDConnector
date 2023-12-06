# ceva_americana
Instructions:
1. Place your Excel workbooks in this directory.
2. Open a terminal and create a virtual environment (python3 -m venv env_rentals) to install the required dependencies (or, if you prefer, install them globally)
3. Activate your virtual environment (source env_rentals/bin/activate)
3. Run `pip install requirements.txt`
4. Open the `.env` file and initialize the environment variable. E.G.  `file = 'Sunset Apartments RR 09122023.xlsx'`
5. Run normalize_rental_sheet.py 
6. Open your workbook and you should see a new sheet called 'transformed_data' 


