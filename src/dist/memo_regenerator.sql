COPY (SELECT * FROM (
    SELECT image.id AS imageId,
           pixels.id AS pixelsId,
           image.series,
           pixelstype.value AS pixelstype,
           pixels.sizeX,
           pixels.sizeY,
           rank() OVER (PARTITION BY fileset.id ORDER BY image.id) AS rank
        FROM fileset
        JOIN image ON fileset.id = image.fileset
        JOIN pixels ON image.id = pixels.image
        JOIN pixelstype ON pixels.pixelstype = pixelstype.id
            WHERE fileset.id = image.fileset
) AS rank WHERE rank.rank = 1) TO STDOUT CSV;
