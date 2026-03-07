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

--公司对应行业表
CREATE TABLE IF NOT EXISTS stock_master_cn (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,      -- A股 6位
  stock_name VARCHAR(64) NULL,
  industry   VARCHAR(64) NULL,
  exchange   ENUM('SSE','SZSE','BSE') NULL,  -- 可选：上交所/深交所/北交所
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_stock_code (stock_code),
  KEY idx_industry (industry),
  KEY idx_exchange (exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;