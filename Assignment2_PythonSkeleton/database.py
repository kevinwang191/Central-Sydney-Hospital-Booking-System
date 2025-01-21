#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''

def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    db_name = "assginment 2"
    userid = ""
    passwd = ""
    myHost = "localhost"


    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=db_name,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn


'''
Validate staff based on username and password
'''

def checkLogin(login, password):
    try:
        conn = openConnection()
        cur = conn.cursor()
        query = """SELECT 
                        UserName, 
                        FirstName, 
                        LastName, 
                        Email 
                    FROM 
                        Administrator 
                    WHERE 
                        LOWER(UserName) = LOWER(%s) AND Password = %s"""
        
        cur.execute(query, (login, password))

        userInfo = cur.fetchone()  

        if userInfo is None:
            return None  

        else:
            return userInfo

    except psycopg2.Error as e:
        print("Database error: ", e)
        return None
    finally:
        if conn is not None:
            conn.close()  

"""
    Convert a list of tuples into a list of dictionaries.
"""

def tuples_to_dicts(tuples_list, column_names):
   
    return [dict(zip(column_names, row)) for row in tuples_list]

'''
List all the associated admissions records in the database by staff
'''

def findAdmissionsByAdmin(login): 
    try:
        conn = openConnection()
        cur = conn.cursor()
        query = """
            SELECT 
                AdmissionID, 
                AdmissionTypeName, 
                DeptName, 
                COALESCE(TO_CHAR(DischargeDate,'DD-MM-YYYY'), '') AS DischargeDate,
                COALESCE(TO_CHAR(Fee,'9999.99'), '') AS Fee, 
                CONCAT(p.FirstName, ' ', p.LastName) AS PatientFullName, 
                COALESCE(Condition, '') AS Condition
            FROM 
                Admission a 
            JOIN 
                AdmissionType ON a.AdmissionType = AdmissionType.AdmissionTypeID
            JOIN 
                Department ON a.Department = Department.DeptId
            JOIN 
                Patient p ON a.Patient = p.PatientID
            WHERE 
                a.Administrator = %s;"""
        
        cur.execute(query, (login,))

        admissions = cur.fetchall()  


        column_names = ['admission_id', 'admission_type', 'admission_department', 'discharge_date', 'fee', 'patient','condition']
        
        admissions_dict = tuples_to_dicts(admissions, column_names)

        return admissions_dict

    except psycopg2.Error as e:
        print("Database error: ", e)
        return None
    
    finally:
        if conn is not None:
            conn.close()  
    return

'''
Find a list of admissions based on the searchString provided as parameter
See assignment description for search specification
'''

def findAdmissionsByCriteria(searchString):
    try:
        conn = openConnection()
        curs = conn.cursor()
        if searchString.strip() == '':

            query = """
                SELECT 
                    AdmissionID, 
                    AdmissionTypeName, 
                    DeptName, 
                    COALESCE(TO_CHAR(DischargeDate,'DD-MM-YYYY'), '') AS DischargeDate, 
                    COALESCE(Fee::text, '') AS Fee,  
                    CONCAT(p.FirstName, ' ', p.LastName) AS PatientFullName,
                    COALESCE(a.Condition, '') AS Condition
                FROM 
                    Admission a
                JOIN 
                    AdmissionType ON a.AdmissionType = AdmissionType.AdmissionTypeID
                JOIN 
                    Department ON a.Department = Department.DeptId
                JOIN 
                    Patient p ON a.Patient = p.PatientID
                WHERE 
                    a.DischargeDate >= CURRENT_DATE - INTERVAL '2 years' OR a.DischargeDate IS NULL
                ORDER BY 
                    a.DischargeDate IS NULL DESC, a.DischargeDate ASC, PatientFullName ASC
                    """
            curs.execute(query)
        else:
            query = f"""
                SELECT 
                    AdmissionID, 
                    AdmissionTypeName, 
                    DeptName, 
                    COALESCE(TO_CHAR(DischargeDate,'DD-MM-YYYY'), '') AS DischargeDate,  
                    COALESCE(Fee::text, '') AS Fee,  
                    CONCAT(p.FirstName, ' ', p.LastName) AS PatientFullName,
                    COALESCE(a.Condition, '') AS Condition
                FROM 
                    Admission a
                JOIN 
                    AdmissionType ON a.AdmissionType = AdmissionType.AdmissionTypeID
                JOIN 
                    Department ON a.Department = Department.DeptId
                JOIN 
                    Patient p ON a.Patient = p.PatientID
                WHERE (
                    AdmissionType.AdmissionTypeName ILIKE '%{searchString}%' OR
                    Department.DeptName ILIKE '%{searchString}%' OR
                    CONCAT(p.FirstName, ' ', p.LastName) ILIKE '%{searchString}%' OR 
                    a.Condition ILIKE '%{searchString}%'
                )
                AND (a.DischargeDate >= CURRENT_DATE - INTERVAL '2 years' OR a.DischargeDate IS NULL)
                ORDER BY 
                    a.DischargeDate IS NULL DESC, a.DischargeDate ASC, PatientFullName ASC 
                """
            curs.execute(query)


        results = curs.fetchall()

        
        column_names = ['admission_id', 'admission_type', 'admission_department', 'discharge_date', 'fee', 'patient','condition']

       

        admissions_dicts = tuples_to_dicts(results, column_names)

        # print("Admission fetched:", results)

        curs.close()
        conn.close()
        return admissions_dicts
    except Exception as e:
        print(e)
    except ValueError:
        print("")

'''
Add a new addmission 
'''

def addAdmission(type, department, patient, condition, admin):
    try:
        conn = openConnection()
        curs = conn.cursor()
        
        sql_call_add = """
                CALL add_admission(%s, %s, %s, %s, %s)
        """
        # print(f"Attempting to add admission with: Type: {type}, Dept: {department}, Patient: {patient}, Condition: {condition}, Admin: {admin}")
        
        curs.execute(sql_call_add, (type, department, patient, condition, admin))
        conn.commit()
        # print(f"Admission with ID {id} has been updated")
        curs.close()
        conn.close()
        return True
    
    except Exception as e:
        print(e)


'''
Update an existing admission
'''

def updateAdmission(id, type, department, dischargeDate, fee, patient, condition):
    cur = None
    try:
        conn = openConnection()
        cur = conn.cursor()
        # print(f"Updating admission with ID: {id}, Type: {type}, Department: {department}, "
            #   f"Discharge Date: {dischargeDate}, Fee: {fee}, Patient: {patient}, Condition: {condition}")

        sql_call_update = """
        CALL update_admission(%s, %s, %s, %s, %s, %s, %s)
        """

        cur.execute(sql_call_update, (id, type, department, dischargeDate, fee, patient, condition))
        conn.commit()
        # print(f"Admission with ID {id} has been updated")

        cur.close()
        conn.close()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.close()
        conn.close()
