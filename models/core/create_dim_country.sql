WITH chanel_country_and_region AS (
    SELECT 
        DISTINCT
        geo.country AS country,
        epk_region AS chanel_region,
    FROM {{ ref('ga4_source_roll_up') }}
    WHERE epk_division_flag = 'fsh'
    AND geo.country IS NOT NULL
),

add_geographical_region AS (
SELECT 
    chanel_country_and_region.*,
    CASE 
        WHEN country IN ('Canada', 'United States', 'USA', 'Mexico') THEN 'North America'
        WHEN country IN ('Cyprus', 'Lebanon', 'Syria', 'Iraq', 'Iran', 'Israel', 'Jordan', 'Saudi Arabia', 'Kuwait', 'Qatar', 'Bahrain', 'United Arab Emirates', 'Oman', 'Yemen') THEN 'Middle East'
        WHEN country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'French Guiana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela') THEN 'Latin America'
        WHEN country IN ('Albania', 'Germany', 'Andorra', 'Austria', 'Belgium', 'Belarus', 'Bosnia and Herzegovina', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Spain', 'Estonia', 'Finland', 'France', 'Greece', 'Hungary', 'Ireland', 'Iceland', 'Italy', 'Kosovo', 'Latvia', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macedonia', 'Painted', 'Moldova', 'Monaco', 'Montenegro', 'Norway', 'Netherlands', 'Poland', 'Portugal', 'Czech Republic', 'Romania', 'United Kingdom', 'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Sweden', 'Switzerland', 'Ukraine', 'Vatican') THEN 'Europe'
        WHEN country IN ('China', 'South Korea', 'Hong Kong', 'Indonesia', 'Malaysia', 'Singapore', 'Taiwan', 'Thailand', 'Japan', 'Australia') THEN 'Asia & Pacific'
        WHEN country IN ('Africa', 'South Africa', 'Algeria', 'Angola', 'Benin', 'Botswana', 'Burkina Faso', 'Burundi', 'Cameroon', 'Cape Verde', 'Central African Republic', 'Comoros', 'Republic of the Congo', 'Democratic Republic of the Congo', 'Ivory Coast', 'Djibouti', 'Egypt', 'Eritrea', 'Eswatini', 'Ethiopia', 'Gabon', 'Gambia', 'Ghana', 'Guinea', 'Guinea-Bissau', 'Equatorial Guinea', 'Kenya', 'Lesotho', 'Liberia', 'Libya', 'Madagascar', 'Malawi', 'Mali', 'Morocco', 'Maurice', 'Mauritania', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Uganda', 'Rwanda', 'Sao Tome-et-Principe', 'Senegal', 'Seychelles', 'Sierra Leone', 'Somalia', 'Sudan', 'South Sudan', 'Tanzania', 'Chad', 'Togo', 'Tunisia', 'Zambia', 'Zimbabwe') THEN 'Africa'
        ELSE 'other'
    END AS geographical_region, -- mapping the geographical region of each country
FROM chanel_country_and_region
)

SELECT * FROM add_geographical_region
