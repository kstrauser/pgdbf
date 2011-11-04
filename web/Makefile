all: pgdbf.1.html upload

upload: *.html *.png *.css favicon.ico pgdbf.1.html
	rsync -av $? sourceforgeweb:/home/project-web/pgdbf/htdocs/
	touch upload

pgdbf.1.html: ../doc/pgdbf.man
	man2html < $? > pgdbf.1.html
