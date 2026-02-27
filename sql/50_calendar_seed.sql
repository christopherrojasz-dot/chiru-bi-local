-- Seed calendario comercial Peru (MVP) - idempotente
-- Requiere que exista el unique index: ux_calendar_event_dates (event_name, start_date, end_date, city)

INSERT INTO analytics.commercial_calendar_pe
(event_name, event_type, start_date, end_date, city, tags, prioridad, notes)
VALUES
('Verano','SEASON','2026-01-01','2026-03-31','Lima',ARRAY['Belleza','Moda','Hogar'],2,'Mayor demanda estacional'),
('Regreso a clases (aprox)','SEASON','2026-02-01','2026-03-31','Lima',ARRAY['Escolar','Bebes'],1,'Ajustar con fechas oficiales de colegios/campanas del negocio'),
('Cyber (ventana aprox)','CAMPAIGN','2026-04-01','2026-04-30','Lima',ARRAY['Tecnologia','Hogar','Moda'],1,'Placeholder: reemplazar por Cyber Wow/Cyber Days reales'),
('Dia de la Madre','HOLIDAY','2026-05-01','2026-05-10','Lima',ARRAY['Moda','Belleza','Hogar'],1,'Semana fuerte retail'),
('Invierno','SEASON','2026-06-01','2026-08-31','Lima',ARRAY['Moda','Hogar'],2,'Mayor demanda estacional'),
('Dia del Padre','HOLIDAY','2026-06-08','2026-06-21','Lima',ARRAY['Tecnologia','Moda','Automotriz'],2,'Semana fuerte retail'),
('Fiestas Patrias','HOLIDAY','2026-07-20','2026-07-29','Lima',ARRAY['Hogar','Tecnologia','Moda'],1,'Pico de consumo'),
('Navidad','HOLIDAY','2026-12-01','2026-12-25','Lima',ARRAY['Tecnologia','Hogar','Moda','Bebes'],1,'Pico maximo')
ON CONFLICT (event_name, start_date, end_date, city) DO NOTHING;