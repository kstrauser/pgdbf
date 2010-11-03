# Ports collection makefile for:	pgdbf
# Date created:				23 June 2009
# Whom:					kirk@daycos.com
#
# $FreeBSD: ports/databases/pgdbf/Makefile,v 1.2 2009/11/26 14:30:50 miwi Exp $
#

PORTNAME=	pgdbf
PORTVERSION=	0.5.2
CATEGORIES=	databases
MASTER_SITES=	SF/${PORTNAME}/${PORTNAME}/${PORTVERSION}

MAINTAINER=	kirk@daycos.com
COMMENT=	Convert XBase / FoxPro tables to PostgreSQL

USE_GMAKE=	yes
GNU_CONFIGURE=	yes

MAN1=		pgdbf.1

PLIST_FILES=	bin/pgdbf \
		share/doc/pgdbf/README
PLIST_DIRS=	%%DOCSDIR%%

.include <bsd.port.mk>
