const url = "https://autowebhook.hooks.digital/webhook/0f5e5fef-5161-42a5-98ea-cf3ba04b744b";
const delay = (ms) => new Promise(r => setTimeout(r, ms));

const empresas = [
    //'Ubiratan Erthal',
    'Conecta Gestão de Relacionamento',
    //'Security Vigilância Eletrônica IA',
    //'Auroque Soluções para Agropecuária',
    //'Mérito Engenharia de Software',
    'Dubai Alimentos',
    'Hidroenergia',
    'Maria Odete Palharini - Criatec',
];

const gerarCPF = () => {
    const body = {
        acao: "gerar_cpf",
        pontuacao: "N",
        cpf_estado: "RS"
    };

    return fetch("https://www.4devs.com.br/ferramentas_online.php", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.4devs.com.br',
            'Referer': 'https://www.4devs.com.br/gerador_de_cpf.php',
        },
        body: new URLSearchParams(body)
    });
};

(async () => {
    for (let i = 0; i < 10000; i++) {
        if (i > 0) await delay(5000);
        try {
            const resCpf = await gerarCPF();
            const cpf = await resCpf.text();
            if (!cpf) {
                console.error('Erro ao gerar CPF');
                continue;
            }

            // Pega empresa correspondente ao índice atual
            const empresa = empresas[i % empresas.length];

            const body = { cpf, nome_empresa: empresa };
            const res = await fetch(url, {
                method: 'POST',
                headers: {
                    'referer': 'https://impulsa.hooks.com.br/',
                    'Authorization': 'Bearer Il~=E8+~QSJ14SaFqnOhNL0zAQCvlKWavIuEVGP9UXbCTgwoL4',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            });

            if (res.ok) {
                console.log(`Enviado um voto de ${cpf} para ${empresa}`);
            } else {
                console.error(`Erro HTTP: ${res.status}`);
            }
        } catch (err) {
            console.error(`Erro ao enviar:`, err);
        }
    }
})();
