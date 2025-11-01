USE hotelbooking;
-- ---------------------------------------------------
--Requêtes SIMPLES

-- Lister tous les clients
SELECT * FROM clients;

-- Lister tous les hôtels à Paris
SELECT *
FROM hotels
WHERE villes = 'Paris';

-- Lister toutes les réservations avec infos hôtels et clients
SELECT r.*, h.nom  AS hotel_nom, c.nom AS client_nom, c.email, c.telephone
FROM reservations r
JOIN hotels h   ON r.hotelid  = h.hotelid
JOIN clients c  ON r.clientid = c.clientid;

-- -------------------------------------------------
-- Requêtes avec JOINTURES & AGRÉGATIONS

-- Nombre de réservations par client
SELECT r.clientid, COUNT(*) AS nb_reservations
FROM reservations r
GROUP BY r.clientid
ORDER BY nb_reservations DESC;

-- Clients qui ont réservé plus que 2 nuitées (somme des nuits > 2)
-- datediff(fin, debut) retourne le nb de jours entre 2 dates (format 'YYYY-MM-DD')
SELECT c.clientid, c.nom,
       SUM(datediff(r.datefin, r.datedebut)) AS total_nuitees
FROM reservations r
JOIN clients c ON r.clientid = c.clientid
GROUP BY c.clientid, c.nom
HAVING SUM(datediff(r.datefin, r.datedebut)) > 2
ORDER BY total_nuitees DESC;

-- Hôtels réservés par chaque client
SELECT c.nom AS client,
       collect_set(h.nom) AS hotels_reserves
FROM reservations r
JOIN clients c ON r.clientid = c.clientid
JOIN hotels  h ON r.hotelid  = h.hotelid
GROUP BY c.nom
ORDER BY client;

-- Noms des hôtels avec plus d’une réservation
SELECT h.nom, COUNT(*) AS nb_reservations
FROM reservations r
JOIN hotels h ON r.hotelid = h.hotelid
GROUP BY h.nom
HAVING COUNT(*) > 1
ORDER BY nb_reservations DESC;

-- Noms des hôtels sans réservation
SELECT h.nom
FROM hotels h
LEFT JOIN reservations r ON h.hotelid = r.hotelid
WHERE r.hotelid IS NULL
ORDER BY h.nom;

-- -------------------------------------------------
-- Requêtes IMBRIQUÉES

-- Clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT DISTINCT c.*
FROM clients c
WHERE c.clientid IN (
  SELECT r.clientid
  FROM reservations r
  JOIN hotels h ON r.hotelid = h.hotelid
  WHERE h.etoiles > 4
);

-- Total des revenus générés par chaque hôtel
SELECT h.nom AS hotel, SUM(r.prixtotal) AS revenus_total
FROM reservations r
JOIN hotels h ON r.hotelid = h.hotelid
GROUP BY h.nom
ORDER BY revenus_total DESC;

-- -------------------------------------------------
--  Agrégations avec partitions/buckets (côté logique)

-- Revenus totaux par ville (partitionnée dans l'énoncé)
SELECT h.villes AS ville, SUM(r.prixtotal) AS revenus
FROM reservations r
JOIN hotels h ON r.hotelid = h.hotelid
GROUP BY h.villes
ORDER BY revenus DESC;

-- Nombre total de réservations par client (table bucketée dans l'énoncé)
SELECT r.clientid, COUNT(*) AS nb_reservations
FROM reservations r
GROUP BY r.clientid
ORDER BY nb_reservations DESC;

-- -------------------------------------------------
-- Nettoyage si besoin (décommenter pour utiliser)
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS clients;
