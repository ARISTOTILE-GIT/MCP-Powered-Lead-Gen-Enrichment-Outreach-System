import faker
import uuid
import random
import database
import sys

fake = faker.Faker()

INDUSTRY_ROLES = {
    "Technology": ["CTO", "VP of Engineering", "Product Manager", "Head of AI", "DevOps Lead"],
    "Healthcare": ["Medical Director", "Clinical Ops Lead", "Procurement Manager", "Head of Patient Experience"],
    "Finance": ["CFO", "Risk Manager", "Investment Lead", "Compliance Officer"],
    "Retail": ["Head of E-commerce", "Supply Chain Manager", "Marketing Director", "Brand Manager"],
    "Manufacturing": ["Plant Manager", "Operations Director", "Logistics Head", "Production Supervisor"]
}

def generate_leads(num_leads):
    print(f"Generating {num_leads} leads into SQLite Database...")
    
    database.init_db()
    
    conn = database.get_db_connection()
    cursor = conn.cursor()
    
    generated_summary = []
    
    for _ in range(num_leads):
        industry = random.choice(list(INDUSTRY_ROLES.keys()))
        role = random.choice(INDUSTRY_ROLES[industry])
        
        first_name = fake.first_name()
        last_name = fake.last_name()
        full_name = f"{first_name} {last_name}"
        company = fake.company()
        
        clean_company = company.replace(' ', '').replace(',', '').replace('.', '').lower()
        website = f"www.{clean_company}.com"
        email = f"{first_name.lower()}.{last_name.lower()}@{clean_company}.com"
        linkedin_url = f"linkedin.com/in/{first_name.lower()}-{last_name.lower()}-{random.randint(100, 999)}"
        
        lead_id = str(uuid.uuid4())
        
        lead_data = (
            lead_id,
            full_name,
            company,
            role,
            industry,
            website,
            email,
            linkedin_url,
            fake.country(),
            "NEW"
        )
        
        cursor.execute('''
            INSERT INTO leads (
                id, full_name, company_name, role, industry, 
                website, email, linkedin_url, country, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', lead_data)
        
        generated_summary.append({"id": lead_id, "full_name": full_name, "company": company})

    conn.commit()
    conn.close()
    
    print(f"Successfully inserted {len(generated_summary)} leads into 'leads.db'.")
    return generated_summary

if __name__ == "__main__":
    generate_leads(10)