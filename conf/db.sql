CREATE TABLE annual_reports (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),
    report_year INT NOT NULL,
    publish_date DATE,
    file_path VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_code_year (stock_code, report_year)
);



CREATE TABLE annual_report_mda (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    report_id BIGINT NOT NULL,
    industry_section LONGTEXT,
    main_business_section LONGTEXT,
    future_section LONGTEXT,
    full_mda LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (report_id) REFERENCES annual_reports(id)
);